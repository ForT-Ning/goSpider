package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/antchfx/htmlquery"
	"github.com/go-redis/redis"
	_ "github.com/go-sql-driver/mysql"
	"github.com/gocolly/colly"
)

var redisdb *redis.Client
var mysqldb *sql.DB
var databaseName string = "test"
var movieTableName string = "movie_table"
var commentTableName string = "comment_table"

//var ctx = context.Background()

/////////////////////////////////////////    initRedis  ///////////////
func initRedis() (err error) {
	redisdb = redis.NewClient(&redis.Options{
		Addr:     "127.0.0.1:6379",
		Password: "666666",
		DB:       0,
	})
	_, err = redisdb.Ping().Result()
	return
} ///////////////////////////////////////// End    initRedis  ///////////////

/////////////////////////////////////////    VisitMovic  ///////////////
func VisitMovic(url string, movieID int) {
	m := colly.NewCollector(
		colly.Async(true),
		colly.UserAgent("Mozilla/5.0 (X11; Ubuntu; Linux aarch64; rv:86.0) Gecko/20100101 Firefox/86.0"),
	)
	m.OnRequest(func(r *colly.Request) {
		fmt.Println("Visiting movic", r.URL)
	})
	//出现错误
	m.OnError(func(_ *colly.Response, err error) {
		fmt.Println("VisitMovic Something went wrong:", err)
	})
	//收到响应后
	m.OnResponse(func(r *colly.Response) {
		//fmt.Println("Response")
		var f interface{}
		json.Unmarshal(r.Body, &f)
		html := f.(map[string]interface{})["html"]
		str := html.(string)
		doc, err := htmlquery.Parse(strings.NewReader(str))
		if err != nil {
			return
		}

		nodes := htmlquery.Find(doc, "//*[@class='comment']")
		for ii, node := range nodes {
			//attr1 := node.Attr("data-id")

			fmt.Println(ii)
			comment := htmlquery.FindOne(node, "//*[@class='short']")
			inputID := htmlquery.FindOne(node, "//input[@value]")

			commentID, err := strconv.Atoi(htmlquery.SelectAttr(inputID, "value"))

			strSelect := fmt.Sprintf("select comment_text from %s where comment_id = %d and comment_c_id = %d", commentTableName, commentID, movieID)
			var strComment string
			err = mysqldb.QueryRow(strSelect).Scan(&strComment)
			if err == sql.ErrNoRows && strComment == "" {
				strComment = htmlquery.InnerText(comment)
				strInsert := fmt.Sprintf("insert into %s(comment_id, comment_c_id, comment_text) value(?,?,?)", commentTableName)
				_, err = mysqldb.Exec(strInsert, commentID, movieID, strComment)
				if err != nil {
					continue
				}
				fmt.Println("insert into comment_table", commentID, strComment)
				// lastInsertID, _ := ret.LastInsertId()
				// fmt.Println("LastInsertID:", lastInsertID)

				// //影响行数
				// rowsaffected, _ := ret.RowsAffected()
				// fmt.Println("RowsAffected:", rowsaffected)
			}
			time.Sleep(time.Second)
		}
	})

	for ii := 0; ii < 20; ii += 20 {
		str := url + "comments?percent_type=&start=" + strconv.Itoa(ii) + "&limit=20&status=P&sort=new_score&comments_only=1"
		m.Visit(str)
	}
	m.Wait()
} ///////////////////////////////////////// End   VisitMovic  ///////////////

func spider() {
	var wg sync.WaitGroup

	c := colly.NewCollector(
		colly.Async(true),
		colly.UserAgent("Mozilla/5.0 (X11; Ubuntu; Linux aarch64; rv:86.0) Gecko/20100101 Firefox/86.0"),
	)
	c.Limit(
		&colly.LimitRule{
			RandomDelay: time.Second,
			Parallelism: 5, //并发数为5
		},
	)

	c.OnRequest(func(r *colly.Request) {
		fmt.Println("Visiting", r.URL)
	})
	//出现错误
	c.OnError(func(_ *colly.Response, err error) {
		fmt.Println("main Something went wrong:", err)
	})
	//收到响应后
	c.OnResponse(func(r *colly.Response) {
		fmt.Println("Response")
		var f interface{}
		json.Unmarshal(r.Body, &f)
		data := f.(map[string]interface{})["data"]
		title := data.([]interface{})
		for _, ok := range title {
			wg.Add(1)
			id, _ := strconv.Atoi(ok.(map[string]interface{})["id"].(string))
			strSelect := fmt.Sprintf("select movie_name from %s where movie_id = %d", movieTableName, id)
			var strTitle string
			err := mysqldb.QueryRow(strSelect).Scan(&strTitle)
			if err == sql.ErrNoRows && strTitle == "" {
				strTitle := ok.(map[string]interface{})["title"].(string)
				strInsert := fmt.Sprintf("insert into %s(movie_id, movie_name) value(?, ?)", movieTableName)
				_, err = mysqldb.Exec(strInsert, id, strTitle)
				if err != nil {
					continue
				}
				fmt.Println("insert movie_table", id, strTitle)
				// lastInsertID, _ := ret.LastInsertId()
				// fmt.Println("LastInsertID:", lastInsertID)

				// //影响行数
				// rowsaffected, _ := ret.RowsAffected()
				// fmt.Println("RowsAffected:", rowsaffected)
			}
			go func(strurl string, movieID int) {
				defer wg.Done()
				VisitMovic(strurl, movieID)
			}((ok.(map[string]interface{})["url"]).(string), id)
			time.Sleep(10 * time.Second)
			//break
		}
	})

	//结束
	c.OnScraped(func(r *colly.Response) {
		fmt.Println("Finished", r.Request.URL)
	})
	//采集开始
	fmt.Println("start")
	for ii := 0; ii < 20; ii += 20 {
		strurl := "https://movie.douban.com/j/new_search_subjects?sort=U&range=0,10&tags=电影&start=" + strconv.Itoa(ii)
		c.Visit(strurl)
	}
	c.Wait()

	wg.Wait()
	fmt.Println("finished")
}

func main() {

	err := initRedis()
	if err != nil {
		fmt.Printf("connect redis failed, err:%v\n", err)
		return
	}

	mysqldb, err = sql.Open("mysql", "root:liang123@/test")
	defer mysqldb.Close()
	if err != nil {
		log.Fatal(err)
	}
	spider()

}
