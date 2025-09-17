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
	case "EXISTS":
		if len(cmd.Args) != 2 {
			conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
			return
		}
		key := string(cmd.Args[1])
		_, err := db.Get(key)
		if err != nil {
			conn.WriteInt(0)
		} else {
			conn.WriteInt(1)
		}

	case "DEL":
		if len(cmd.Args) < 2 {
			conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
			return
		}
		count := 0
		for i := 1; i < len(cmd.Args); i++ {
			key := string(cmd.Args[i])
			if _, err := db.Get(key); err == nil {
				err := db.Delete(key)
				if err != nil {
					return
				}
				count++
			}
		}
		conn.WriteInt(count)
	case "RPOP":
		if len(cmd.Args) != 2 {
			conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
			return
		}
		key := string(cmd.Args[1])
		val, err := db.RPop(key) // 你需要实现 LPop
		if err != nil {
			conn.WriteNull()
		} else {
			conn.WriteBulkString(val)
		}

	case "LLEN":
		if len(cmd.Args) != 2 {
			conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
			return
		}
		key := string(cmd.Args[1])
		length := db.LLen(key) // 你需要实现 LLen
		conn.WriteInt(length)
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

}
