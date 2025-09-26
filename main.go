package main

import (
	"fmt"
	"github.com/tidwall/redcon"
	"goludis/cache"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

var db *cache.BucketCache[string, string]

type Unconfirmed struct {
	cmd   string
	keyId string
	val   string
}

type LudisParams struct {
	Db  int
	Unc *Unconfirmed
}

const defaultDB = 0

func acceptHandler(conn redcon.Conn) bool {
	conn.SetContext(&LudisParams{
		Db:  defaultDB,
		Unc: nil,
	})
	return true
}

func currentDB(conn redcon.Conn) int {
	if v := conn.Context(); v != nil {
		return v.(*LudisParams).Db
	}
	return defaultDB
}

func dbKey(conn redcon.Conn, key string) (string, string) {
	return key, fmt.Sprintf("%d:%s", currentDB(conn), key)
}

func writeZSetReply(conn redcon.Conn, members []interface{}, withScores bool) {
	if len(members) == 0 {
		conn.WriteArray(0)
		return
	}
	if len(members)%2 != 0 {
		conn.WriteError("ERR wrong number of arguments for  writeZSetReply")
		conn.WriteArray(0)
		return
	}
	if !withScores {
		conn.WriteArray(len(members) / 2)
	} else {
		conn.WriteArray(len(members))
	}

	for index, v := range members {
		if !withScores && index%2 != 0 {
			continue
		}
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

	//fmt.Println("start")
	//for i := range len(cmd.Args) {
	//	fmt.Println("arg", string(cmd.Args[i]))
	//}
	//fmt.Println("end")

	switch strings.ToUpper(string(cmd.Args[0])) {

	case "SELECT":
		if len(cmd.Args) != 2 {
			conn.WriteError("ERR wrong number of arguments for 'select'")
			return
		}
		idx, err := strconv.Atoi(string(cmd.Args[1]))
		if err != nil || idx < 0 {
			conn.WriteError("ERR invalid DB index")
			return
		}

		ludisParams := conn.Context().(*LudisParams)
		ludisParams.Db = idx
		conn.SetContext(ludisParams)
		log.Printf("client switched to DB %d", idx)
		conn.WriteString("OK")

		return

	case "SET":
		if len(cmd.Args) != 3 && len(cmd.Args) != 5 {
			conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
			return
		}
		_, keyId := dbKey(conn, string(cmd.Args[1]))
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
		err := db.Set(keyId, value, opt)
		if err != nil {
			conn.WriteError("SET error: " + err.Error())
		}
		conn.WriteString("OK")

	case "GET":
		if len(cmd.Args) != 2 {
			fmt.Println("wrong GET")

			conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
			return
		}
		_, keyId := dbKey(conn, string(cmd.Args[1]))
		val, err := db.Get(keyId)
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
		_, keyId := dbKey(conn, string(cmd.Args[1]))
		for i := 2; i < len(cmd.Args); i++ {
			db.LPush(keyId, string(cmd.Args[i]))
		}
		conn.WriteInt(len(cmd.Args) - 2)
	case "BRPOP":
		if len(cmd.Args) != 3 {
			fmt.Println("BRPOP")
			conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
			return
		}
		key, keyId := dbKey(conn, string(cmd.Args[1]))

		timeoutSec, err := strconv.Atoi(string(cmd.Args[2]))
		if err != nil {
			conn.WriteError("ERR invalid timeout time")
			return
		}
		val, err := db.BRPop(keyId, time.Duration(timeoutSec)*time.Second)
		if err != nil {
			conn.WriteNull()
		} else {
			if p, ok := conn.Context().(*LudisParams); ok {
				p.Unc = &Unconfirmed{cmd: "BRPOP", keyId: keyId, val: val}
			}
			conn.WriteArray(2)
			conn.WriteBulkString(key)
			conn.WriteBulkString(val)
		}
	case "EXISTS":
		if len(cmd.Args) != 2 {
			conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
			return
		}
		_, keyId := dbKey(conn, string(cmd.Args[1]))
		_, err := db.Get(keyId)
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
			_, keyId := dbKey(conn, string(cmd.Args[i]))

			if _, err := db.Get(keyId); err == nil {
				err := db.Delete(keyId)
				if err != nil {
					continue
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
		_, keyId := dbKey(conn, string(cmd.Args[1]))

		val, err := db.RPop(keyId)
		if err != nil {
			conn.WriteNull()
		} else {
			if p, ok := conn.Context().(*LudisParams); ok {
				p.Unc = &Unconfirmed{cmd: "RPOP", keyId: keyId, val: val}
			}
			conn.WriteBulkString(val)
		}

	case "LLEN":
		if len(cmd.Args) != 2 {
			conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
			return
		}
		_, keyId := dbKey(conn, string(cmd.Args[1]))
		length := db.LLen(keyId)
		conn.WriteInt(length)
	case "ZADD":
		if len(cmd.Args) < 4 || (len(cmd.Args)-2)%2 != 0 {
			conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
			return
		}
		_, keyId := dbKey(conn, string(cmd.Args[1]))
		added := 0
		for i := 2; i < len(cmd.Args); i += 2 {
			score, err := strconv.ParseFloat(string(cmd.Args[i]), 64)
			if err != nil {
				conn.WriteError("ERR wrong score: " + err.Error())
				return
			}
			member := string(cmd.Args[i+1])
			n, err := db.ZAdd(keyId, score, member)
			added += n
		}
		conn.WriteInt(added)

	case "ZREM":
		if len(cmd.Args) < 3 {
			conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
			return
		}
		_, keyId := dbKey(conn, string(cmd.Args[1]))
		removed := 0
		for i := 2; i < len(cmd.Args); i++ {
			removed += db.ZRem(keyId, string(cmd.Args[i]))
		}
		conn.WriteInt(removed)

	case "ZSCORE":
		if len(cmd.Args) != 3 {
			conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
			return
		}
		_, keyId := dbKey(conn, string(cmd.Args[1]))
		score, ok := db.ZScore(keyId, string(cmd.Args[2]))
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
		_, keyId := dbKey(conn, string(cmd.Args[1]))
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
		member := db.ZRange(keyId, start, stop)
		writeZSetReply(conn, any(member).([]interface{}), withScores)

	case "ZREVRANGE":
		if len(cmd.Args) < 4 {
			conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
			return
		}
		_, keyId := dbKey(conn, string(cmd.Args[1]))
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
		member := db.ZRevRange(keyId, start, stop)
		writeZSetReply(conn, any(member).([]interface{}), withScores)

	case "ZRANGEBYSCORE":
		if len(cmd.Args) < 4 {
			conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
			return
		}
		_, keyId := dbKey(conn, string(cmd.Args[1]))
		args := make([]string, len(cmd.Args)-2)
		for i := 2; i < len(cmd.Args); i++ {
			args[i-2] = string(cmd.Args[i])
		}
		members, err := db.ZRangeByScore(keyId, args...)
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
		_, keyId := dbKey(conn, string(cmd.Args[1]))
		args := make([]string, len(cmd.Args)-2)
		for i := 2; i < len(cmd.Args); i++ {
			args[i-2] = string(cmd.Args[i])
		}
		members, err := db.ZRevRangeByScore(keyId, args...)
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

	case "PING":
		if len(cmd.Args) == 1 {
			conn.WriteString("PONG")
		} else {
			conn.WriteBulkString(string(cmd.Args[1]))
		}

	case "INFO":
		info := "# Server\r\nredis_version:7.0.0\r\nredis_mode:standalone\r\n"
		conn.WriteBulkString(info)

	case "CLIENT":
		if len(cmd.Args) >= 2 && strings.ToUpper(string(cmd.Args[1])) == "SETNAME" {
			conn.WriteString("OK")
		} else {
			conn.WriteError("ERR unknown CLIENT subcommand")
		}

	case "CLUSTER":
		if len(cmd.Args) >= 2 && strings.ToUpper(string(cmd.Args[1])) == "SLOTS" {
			conn.WriteArray(0)
		} else {
			conn.WriteError("ERR unknown CLUSTER subcommand")
		}

	case "COMMAND":
		conn.WriteArray(0)

	default:
		conn.WriteError(fmt.Sprintf("ERR unknown command '%s'", string(cmd.Args[0])))
	}

}

func main() {
	if len(os.Args) < 2 {
		_, err := fmt.Fprintf(os.Stderr, "Usage: %s <config-file>\n", os.Args[0])
		if err != nil {
			panic(err)
		}
		os.Exit(1)
	}
	configPath := os.Args[1]
	fmt.Println("Loading config from", configPath)
	var err error

	config, err := ReadConfig(configPath)
	if err != nil {
		panic(err)
	}
	db, err = cache.NewCache[string, string](config.Shards)
	if err != nil {
		panic(err)
	}

	log.Println("Starting Redis-compatible server on", config.Addr)
	err = redcon.ListenAndServe(config.Addr,
		handleRedisCommand,
		acceptHandler,
		func(conn redcon.Conn, err error) {
			if err == nil {
				return
			}
			if p, ok := conn.Context().(*LudisParams); ok && p.Unc != nil {
				u := p.Unc
				db.LPush(u.keyId, u.val)
				log.Printf("%s rollback: %q pushed back to %s (conn %s closed: %v)",
					u.cmd, u.val, u.keyId, conn.RemoteAddr(), err)
			}
		},
	)
	if err != nil {
		log.Fatal(err)
	}

}
