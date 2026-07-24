package pgwire

import (
	"context"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"

	"github.com/jackc/pgproto3/v2"

	ourparser "github.com/Anmol202005/VScale/internal/parser"
	"github.com/Anmol202005/VScale/internal/gate/gateway"
	pb "github.com/Anmol202005/VScale/proto/tablet"
)

const (
	pgTypeText    = 25
	pgTypeInt4    = 23
	pgTypeInt8    = 20
	pgTypeFloat8  = 701
	pgTypeBool    = 16
	pgTypeUnknown = 705
)

type Conn struct {
	conn    net.Conn
	backend *pgproto3.Backend
	gw      *gateway.Gateway
	txID    int64
	ctx     context.Context
	cancel  context.CancelFunc
}

func NewConn(nc net.Conn, gw *gateway.Gateway) *Conn {
	ctx, cancel := context.WithCancel(context.Background())
	return &Conn{
		conn:    nc,
		backend: pgproto3.NewBackend(pgproto3.NewChunkReader(nc), nc),
		gw:      gw,
		ctx:     ctx,
		cancel:  cancel,
	}
}

func (c *Conn) Run() {
	defer c.close()

	if err := c.handleStartup(); err != nil {
		log.Printf("pgwire: startup failed: %v", err)
		return
	}

	for {
		if c.ctx.Err() != nil {
			return
		}

		msg, err := c.backend.Receive()
		if err != nil {
			if !isNetClosed(err) {
				log.Printf("pgwire: receive error: %v", err)
			}
			return
		}

		switch m := msg.(type) {
		case *pgproto3.Query:
			if err := c.handleQuery(string(m.String)); err != nil {
				log.Printf("pgwire: query handler error: %v", err)
				return
			}
		case *pgproto3.Terminate:
			return
		case *pgproto3.Parse:
			c.sendParseComplete()
		case *pgproto3.Bind:
			c.sendBindComplete()
		case *pgproto3.Execute:
			c.sendError(fmt.Errorf("prepared statements not yet supported"))
		case *pgproto3.Sync:
			c.sendReadyForQuery()
		case *pgproto3.Close:
			c.sendCloseComplete()
		default:
			log.Printf("pgwire: unhandled message type %T", msg)
		}
	}
}

func (c *Conn) handleStartup() error {
	for {
		msg, err := c.backend.ReceiveStartupMessage()
		if err != nil {
			return err
		}

		switch m := msg.(type) {
		case *pgproto3.StartupMessage:
			_ = m
			if err := c.sendAuthOK(); err != nil {
				return err
			}
			if err := c.sendParameterStatus("server_version", "14.0"); err != nil {
				return err
			}
			if err := c.sendParameterStatus("DateStyle", "ISO, MDY"); err != nil {
				return err
			}
			if err := c.sendReadyForQuery(); err != nil {
				return err
			}
			return nil
		case *pgproto3.SSLRequest:
			if _, err := c.conn.Write([]byte("N")); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unknown startup message: %T", msg)
		}
	}
}

func (c *Conn) handleQuery(sql string) error {
	sql = strings.TrimSpace(sql)
	if sql == "" {
		return c.backend.Send(&pgproto3.EmptyQueryResponse{})
	}

	stmts, err := ourparser.Parse(sql)
	if err != nil {
		return c.sendError(err)
	}
	if len(stmts) == 0 {
		return c.sendError(fmt.Errorf("empty statement"))
	}

	stmt := stmts[0]

	if ourparser.IsBegin(stmt) {
		return c.handleBegin()
	}
	if ourparser.IsCommit(stmt) {
		return c.handleCommit()
	}
	if ourparser.IsRollback(stmt) {
		return c.handleRollback()
	}

	resp, err := c.gw.Execute(c.ctx, &pb.QueryRequest{Sql: sql, TransactionId: c.txID})
	if err != nil {
		return c.sendError(err)
	}

	if len(resp.Results) == 0 {
		return c.backend.Send(&pgproto3.EmptyQueryResponse{})
	}

	for _, qr := range resp.Results {
		if err := c.sendResult(qr); err != nil {
			return err
		}
	}

	return c.sendReadyForQuery()
}

func (c *Conn) handleBegin() error {
	resp, err := c.gw.Execute(c.ctx, &pb.QueryRequest{Sql: "BEGIN", TransactionId: c.txID})
	if err != nil {
		return c.sendError(err)
	}
	c.txID = resp.TransactionId

	cc := &pgproto3.CommandComplete{CommandTag: []byte("BEGIN")}
	if err := c.backend.Send(cc); err != nil {
		return err
	}
	return c.sendReadyForQueryTx()
}

func (c *Conn) handleCommit() error {
	resp, err := c.gw.Execute(c.ctx, &pb.QueryRequest{Sql: "COMMIT", TransactionId: c.txID})
	if err != nil {
		return c.sendError(err)
	}
	c.txID = resp.TransactionId

	cc := &pgproto3.CommandComplete{CommandTag: []byte("COMMIT")}
	if err := c.backend.Send(cc); err != nil {
		return err
	}
	return c.sendReadyForQuery()
}

func (c *Conn) handleRollback() error {
	resp, err := c.gw.Execute(c.ctx, &pb.QueryRequest{Sql: "ROLLBACK", TransactionId: c.txID})
	if err != nil {
		return c.sendError(err)
	}
	c.txID = resp.TransactionId

	cc := &pgproto3.CommandComplete{CommandTag: []byte("ROLLBACK")}
	if err := c.backend.Send(cc); err != nil {
		return err
	}
	return c.sendReadyForQuery()
}

func (c *Conn) sendResult(qr *pb.QueryResult) error {
	if len(qr.Columns) == 0 {
		cc := &pgproto3.CommandComplete{CommandTag: []byte(fmt.Sprintf("%s %d", qr.Sql, qr.RowsAffected))}
		return c.backend.Send(cc)
	}

	fields := make([]pgproto3.FieldDescription, len(qr.Columns))
	for i, col := range qr.Columns {
		fields[i] = pgproto3.FieldDescription{
			Name:         []byte(col),
			TableOID:     0,
			TableAttributeNumber: 0,
			DataTypeOID:  inferOID(qr, i),
			DataTypeSize: -1,
			TypeModifier: -1,
			Format:       0,
		}
	}

	if err := c.backend.Send(&pgproto3.RowDescription{Fields: fields}); err != nil {
		return err
	}

	for _, row := range qr.Rows {
		dr := &pgproto3.DataRow{Values: make([][]byte, len(row.Values))}
		for i, v := range row.Values {
			dr.Values[i] = []byte(v)
		}
		if err := c.backend.Send(dr); err != nil {
			return err
		}
	}

	tag := qr.Sql
	if tag == "" {
		tag = "SELECT"
	}
	cc := &pgproto3.CommandComplete{CommandTag: []byte(fmt.Sprintf("%s %d", tag, len(qr.Rows)))}
	if err := c.backend.Send(cc); err != nil {
		return err
	}

	return nil
}

func (c *Conn) sendAuthOK() error {
	return c.backend.Send(&pgproto3.AuthenticationOk{})
}

func (c *Conn) sendParameterStatus(name, value string) error {
	return c.backend.Send(&pgproto3.ParameterStatus{Name: name, Value: value})
}

func (c *Conn) sendReadyForQuery() error {
	return c.backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
}

func (c *Conn) sendReadyForQueryTx() error {
	return c.backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'T'})
}

func (c *Conn) sendParseComplete() error {
	return c.backend.Send(&pgproto3.ParseComplete{})
}

func (c *Conn) sendBindComplete() error {
	return c.backend.Send(&pgproto3.BindComplete{})
}

func (c *Conn) sendCloseComplete() error {
	return c.backend.Send(&pgproto3.CloseComplete{})
}

func (c *Conn) sendError(err error) error {
	msg := &pgproto3.ErrorResponse{
		Severity: "ERROR",
		Code:     "XX000",
		Message:  err.Error(),
	}
	if err := c.backend.Send(msg); err != nil {
		return err
	}
	return c.sendReadyForQuery()
}

func (c *Conn) close() {
	c.cancel()
	c.conn.Close()
}

func isNetClosed(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "use of closed network connection") ||
		strings.Contains(err.Error(), " broken pipe") ||
		strings.Contains(err.Error(), "connection reset by peer")
}

func inferOID(qr *pb.QueryResult, colIdx int) uint32 {
	if len(qr.Rows) == 0 {
		return pgTypeText
	}
	v := qr.Rows[0].Values[colIdx]
	if v == "" {
		return pgTypeText
	}
	if _, err := strconv.ParseInt(v, 10, 64); err == nil {
		return pgTypeInt8
	}
	if _, err := strconv.ParseFloat(v, 64); err == nil {
		return pgTypeFloat8
	}
	if _, err := strconv.ParseBool(v); err == nil {
		return pgTypeBool
	}
	return pgTypeText
}
