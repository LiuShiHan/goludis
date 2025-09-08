package main

import (
	"fmt"
	"github.com/tidwall/redcon"
	"goludis/cache"
	"log"
	"strconv"
	"strings"
	"time"
)

var db *cache.BucketCache[string, string]

func handleRedisCommand(conn redcon.Conn, cmd redcon.Command) {
	//for _, arg := range cmd.Args {
	//	fmt.Println("handleRedisCommand", string(arg))
	//}
	//fmt.Println("handleRedisCommand", cmd.Args)
	switch strings.ToUpper(string(cmd.Args[0])) {
	case "SET":

		if len(cmd.Args) != 3 && len(cmd.Args) != 5 {
			conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
			return
		}
		key := string(cmd.Args[1])
		value := string(cmd.Args[2])
		var opt *cache.SetOptions
		if len(cmd.Args) == 5 && strings.ToUpper(string(cmd.Args[3])) == "EX" {
			ttlSec, err := strconv.Atoi(string(cmd.Args[4]))
			if err != nil {
				conn.WriteError("ERR invalid expire time")
				return
			}
			opt = &cache.SetOptions{
				Expires: true,
				TTL:     time.Duration(ttlSec) * time.Second,
			}
		}
		err := db.Set(key, value, opt)
		if err != nil {
			conn.WriteError("SET error: " + err.Error())
		}
		conn.WriteString("OK")

	case "GET":
		if len(cmd.Args) != 2 {
			fmt.Println("GET")
			conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
			return
		}
		key := string(cmd.Args[1])
		val, err := db.Get(key)
		if err != nil {
			conn.WriteNull()
		} else {
			conn.WriteBulkString(val)
		}
	case "LPUSH":
		if len(cmd.Args) < 3 {
			fmt.Println("LPUSH")
			conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
			return
		}
		key := string(cmd.Args[1])
		for i := 2; i < len(cmd.Args); i++ {
			db.LPush(key, string(cmd.Args[i]))
		}
		conn.WriteInt(len(cmd.Args) - 2)
	case "BRPOP":
		if len(cmd.Args) != 3 {
			fmt.Println("BRPOP")
			conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
			return
		}
		key := string(cmd.Args[1])
		timeoutSec, err := strconv.Atoi(string(cmd.Args[2]))
		if err != nil {
			conn.WriteError("ERR invalid timeout time")
			return
		}
		val, err := db.BRPop(key, time.Duration(timeoutSec)*time.Second)
		if err != nil {
			conn.WriteNull()
		} else {
			conn.WriteArray(2)
			conn.WriteBulkString(key)
			conn.WriteBulkString(val)
		}
	default:
		conn.WriteError(fmt.Sprintf("ERR unknown command '%s'", string(cmd.Args[0])))
	}
}

func main() {

	var err error
	db, err = cache.NewCache[string, string](4)
	if err != nil {
		panic(err)
	}

	addr := ":6379"
	log.Println("Starting Redis-compatible server on", addr)
	err = redcon.ListenAndServe(addr,
		handleRedisCommand,
		func(conn redcon.Conn) bool { return true },
		func(conn redcon.Conn, err error) { log.Println("closed:", err) },
	)
	if err != nil {
		log.Fatal(err)
	}

	//var addr = ":6380"
	//var mu sync.RWMutex
	//var items = make(map[string][]byte)
	//var ps redcon.PubSub
	//go log.Printf("started server at %s", addr)
	//err := redcon.ListenAndServe(addr,
	//	func(conn redcon.Conn, cmd redcon.Command) {
	//		switch strings.ToLower(string(cmd.Args[0])) {
	//		default:
	//			conn.WriteError("ERR unknown command '" + string(cmd.Args[0]) + "'")
	//		case "ping":
	//			conn.WriteString("PONG")
	//		case "quit":
	//			conn.WriteString("OK")
	//			conn.Close()
	//		case "set":
	//			if len(cmd.Args) != 3 {
	//				conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
	//				return
	//			}
	//			mu.Lock()
	//			items[string(cmd.Args[1])] = cmd.Args[2]
	//			mu.Unlock()
	//			conn.WriteString("OK")
	//		case "get":
	//			if len(cmd.Args) != 2 {
	//				conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
	//				return
	//			}
	//			mu.RLock()
	//			val, ok := items[string(cmd.Args[1])]
	//			mu.RUnlock()
	//			if !ok {
	//				conn.WriteNull()
	//			} else {
	//				conn.WriteBulk(val)
	//			}
	//		case "setnx":
	//			if len(cmd.Args) != 3 {
	//				conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
	//				return
	//			}
	//			mu.RLock()
	//			_, ok := items[string(cmd.Args[1])]
	//			mu.RUnlock()
	//			if ok {
	//				conn.WriteInt(0)
	//				return
	//			}
	//			mu.Lock()
	//			items[string(cmd.Args[1])] = cmd.Args[2]
	//			mu.Unlock()
	//			conn.WriteInt(1)
	//		case "del":
	//			if len(cmd.Args) != 2 {
	//				conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
	//				return
	//			}
	//			mu.Lock()
	//			_, ok := items[string(cmd.Args[1])]
	//			delete(items, string(cmd.Args[1]))
	//			mu.Unlock()
	//			if !ok {
	//				conn.WriteInt(0)
	//			} else {
	//				conn.WriteInt(1)
	//			}
	//		case "publish":
	//			if len(cmd.Args) != 3 {
	//				conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
	//				return
	//			}
	//			conn.WriteInt(ps.Publish(string(cmd.Args[1]), string(cmd.Args[2])))
	//		case "subscribe", "psubscribe":
	//			if len(cmd.Args) < 2 {
	//				conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
	//				return
	//			}
	//			command := strings.ToLower(string(cmd.Args[0]))
	//			for i := 1; i < len(cmd.Args); i++ {
	//				if command == "psubscribe" {
	//					ps.Psubscribe(conn, string(cmd.Args[i]))
	//				} else {
	//					ps.Subscribe(conn, string(cmd.Args[i]))
	//				}
	//			}
	//		}
	//	},
	//	func(conn redcon.Conn) bool {
	//		// Use this function to accept or deny the connection.
	//		// log.Printf("accept: %s", conn.RemoteAddr())
	//		return true
	//	},
	//	func(conn redcon.Conn, err error) {
	//		// This is called when the connection has been closed
	//		// log.Printf("closed: %s, err: %v", conn.RemoteAddr(), err)
	//	},
	//)
	//if err != nil {
	//	log.Fatal(err)
	//}

}
