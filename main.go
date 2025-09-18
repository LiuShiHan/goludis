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

func writeZSetReply(conn redcon.Conn, members []interface{}, withScores bool) {
	if len(members) == 0 {
		conn.WriteArray(0)
		return
	}
	conn.WriteArray(len(members))
	for _, v := range members {
		switch val := v.(type) {
		case string:
			conn.WriteBulkString(val)
		case float64:
			conn.WriteBulkString(strconv.FormatFloat(val, 'f', -1, 64))
		default:
			conn.WriteBulkString(fmt.Sprint(v))
		}
	}
}

func handleRedisCommand(conn redcon.Conn, cmd redcon.Command) {

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
		val, err := db.RPop(key)
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
		length := db.LLen(key)
		conn.WriteInt(length)
	case "ZADD":
		if len(cmd.Args) < 4 || (len(cmd.Args)-2)%2 != 0 {
			conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
			return
		}
		key := string(cmd.Args[1])
		added := 0
		for i := 2; i < len(cmd.Args); i += 2 {
			score, err := strconv.ParseFloat(string(cmd.Args[i]), 64)
			if err != nil {
				conn.WriteError("ERR wrong score: " + err.Error())
				return
			}
			member := string(cmd.Args[i+1])
			n, err := db.ZAdd(key, score, member)
			added += n
		}
		conn.WriteInt(added)

	case "ZREM":
		if len(cmd.Args) < 3 {
			conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
			return
		}
		key := string(cmd.Args[1])
		removed := 0
		for i := 2; i < len(cmd.Args); i++ {
			removed += db.ZRem(key, string(cmd.Args[i]))
		}
		conn.WriteInt(removed)

	case "ZSCORE":
		if len(cmd.Args) != 3 {
			conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
			return
		}
		score, ok := db.ZScore(string(cmd.Args[1]), string(cmd.Args[2]))
		if !ok {
			conn.WriteNull()
		} else {
			conn.WriteBulkString(strconv.FormatFloat(score, 'f', -1, 64))
		}

	case "ZRANGE":
		if len(cmd.Args) < 4 {
			conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
			return
		}
		key := string(cmd.Args[1])
		start, err := strconv.Atoi(string(cmd.Args[2]))
		if err != nil {
			conn.WriteError("ERR wrong start index: " + err.Error())
			return
		}
		stop, err := strconv.Atoi(string(cmd.Args[3]))
		if err != nil {
			conn.WriteError("ERR wrong stop index: " + err.Error())
			return
		}
		withScores := len(cmd.Args) == 5 && strings.ToUpper(string(cmd.Args[4])) == "WITHSCORES"
		member := db.ZRange(key, start, stop)
		writeZSetReply(conn, any(member).([]interface{}), withScores)

	case "ZREVRANGE":
		if len(cmd.Args) < 4 {
			conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
			return
		}
		key := string(cmd.Args[1])
		start, err := strconv.Atoi(string(cmd.Args[2]))
		if err != nil {
			conn.WriteError("ERR wrong start index: " + err.Error())
			return
		}
		stop, err := strconv.Atoi(string(cmd.Args[3]))
		if err != nil {
			conn.WriteError("ERR wrong stop index: " + err.Error())
		}
		withScores := len(cmd.Args) == 5 && strings.ToUpper(string(cmd.Args[4])) == "WITHSCORES"
		member := db.ZRevRange(key, start, stop)
		writeZSetReply(conn, any(member).([]interface{}), withScores)

	case "ZRANGEBYSCORE":
		if len(cmd.Args) < 4 {
			conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
			return
		}
		key := string(cmd.Args[1])
		args := make([]string, len(cmd.Args)-2)
		for i := 2; i < len(cmd.Args); i++ {
			args[i-2] = string(cmd.Args[i])
		}
		members, err := db.ZRangeByScore(key, args...)
		if err != nil {
			conn.WriteError("ERR " + err.Error())
			return
		}
		withScores := false
		for _, a := range args {
			if strings.ToUpper(a) == "WITHSCORES" {
				withScores = true
				break
			}
		}
		writeZSetReply(conn, members, withScores)

	case "ZREVRANGEBYSCORE":
		if len(cmd.Args) < 4 {
			conn.WriteError("ERR wrong number of arguments for 'zrevrangebyscore' command")
			return
		}
		key := string(cmd.Args[1])
		args := make([]string, len(cmd.Args)-2)
		for i := 2; i < len(cmd.Args); i++ {
			args[i-2] = string(cmd.Args[i])
		}
		members, err := db.ZRevRangeByScore(key, args...)
		if err != nil {
			conn.WriteError("ERR " + err.Error())
			return
		}
		withScores := false
		for _, a := range args {
			if strings.ToUpper(a) == "WITHSCORES" {
				withScores = true
				break
			}
		}
		writeZSetReply(conn, members, withScores)

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
