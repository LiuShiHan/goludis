package network

import (
	"bytes"
	"github.com/tidwall/redcon"
	"strconv"
)

type TxConn struct {
	Orig redcon.Conn
	Buf  bytes.Buffer
}

func (c *TxConn) RemoteAddr() string     { return c.Orig.RemoteAddr() }
func (c *TxConn) Close() error           { return nil }
func (c *TxConn) WriteError(msg string)  { c.writeRaw([]byte("-ERR " + msg + "\r\n")) }
func (c *TxConn) WriteString(str string) { c.writeRaw([]byte("+" + str + "\r\n")) }
func (c *TxConn) WriteBulkString(bulk string) {
	c.writeRaw([]byte("$" + strconv.Itoa(len(bulk)) + "\r\n" + bulk + "\r\n"))
}
func (c *TxConn) WriteInt(num int) {
	c.writeRaw([]byte(":" + strconv.Itoa(num) + "\r\n"))
}
func (c *TxConn) WriteArray(count int) {
	c.writeRaw([]byte("*" + strconv.Itoa(count) + "\r\n"))
}
func (c *TxConn) WriteRaw(data []byte) { c.writeRaw(data) }
func (c *TxConn) writeRaw(p []byte)    { c.Buf.Write(p) }

func (c *TxConn) SetContext(ctx interface{}) {}
func (c *TxConn) Context() interface{}       { return c.Orig.Context() }
