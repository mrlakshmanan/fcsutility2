package fcsutility2

import (
	"archive/zip"
	"bufio"
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	_ "github.com/denisenkom/go-mssqldb"
	_ "github.com/go-sql-driver/mysql"
)

const (
	Statement = "1"
	Detail    = "2"
	Panic     = "P"
	NoPanic   = "NP"
	INSERT    = "INSERT"
	UPDATE    = "UPDATE"
)

type Database struct {
	Server   string
	Port     string
	Database string
	User     string
	Password string
	DbType   string
}

type Behaviour struct {
	Debug        string
	DebugLevel   string
	DisplayDebug string
	ProgramId    string
	ProgramUser  string
	ProgramName  string
}

type UploadMaster struct {
	Summary          string
	SourceCode       string
	IsDefault        string
	IsForceDefault   string
	ZohoDepartmentId string
	UploadFileName   string
	UploadedBy       string
	CampaignId       string
	IsCampaign       string
}

type TicketLog struct {
	Id                 string
	ClientId           string
	Description        string
	Source             string
	SummaryUser        string
	TicketStatus       string
	ZohoDepartmentId   string
	UploadDataMasterId int
	AssigneeId         string
	Spawned            bool
	Processed          bool
	STCode             string
	CreatedBy          string
	CreatedProgram     string
	UpdatedBy          string
	UpdatedProgram     string
}

type EmailLogType struct {
	Action         string
	Id             string
	EmailServer    string
	Type           string
	From           string
	FromDspName    string
	To             string
	Cc             string
	Bcc            string
	ReplyTo        string
	Subject        string
	Body           string
	CreationDate   string
	SentDate       string
	Status         string
	ErrorMsg       string
	CreatedProgram string
}

type myCloser interface {
	Close() error
}

//--------------------------------------------------------------------
// function establishes DB connection
//--------------------------------------------------------------------
func Getdb(dbtype string, dbdtl Database) (*sql.DB, error) {
	// Connect to database
	connString := ""
	if dbtype == "mssql" {

		connString = fmt.Sprintf("server=%s;user id=%s;password=%s;port=%s;database=%s;",
			dbdtl.Server, dbdtl.User, dbdtl.Password, dbdtl.Port, dbdtl.Database)
	} else if dbtype == "mysql" {
		// Connect to database
		port, _ := strconv.Atoi(dbdtl.Port)
		connString = fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", dbdtl.User, dbdtl.Password, dbdtl.Server, port, dbdtl.Database)
	}
	db, err := sql.Open(dbtype, connString)

	return db, err
}

//--------------------------------------------------------------------
// function to insert debug message into db
//--------------------------------------------------------------------
func Debug(db *sql.DB, defaultdisp string, debug string, debuglevel string, pdebuglevel string, programid string, program string, msg string) {
	if defaultdisp == "Y" {
		log.Println(msg)
	}
	vn_debuglevel, _ := strconv.Atoi(debuglevel)
	vn_pdebuglevel, _ := strconv.Atoi(pdebuglevel)

	if debug == "Y" && vn_debuglevel >= vn_pdebuglevel {
		sqlString := "insert into debugtbl(debugtime,programid,programName,debugmsg) values(?,?,?,?)"
		_, inserterr := db.Exec(sqlString, time.Now(), programid, program, msg)
		//sqlString := "insert into debugtbl(debugtime,programid,programName,debugmsg) values(GETDATE(),'" + programid + "','" + program + "','" + msg + "')"
		//_, inserterr := db.Exec(sqlString)
		if inserterr != nil {
			LogError(NoPanic, inserterr.Error())
		}
	}

}

//--------------------------------------------------------------------
// function to insert debug message into db
//--------------------------------------------------------------------
func Error(db *sql.DB, programid string, program string, msg string) {
	sqlString := "insert into errortbl(errortime,programid,programName,errormsg) values(?,?,?,?)"
	_, inserterr := db.Exec(sqlString, time.Now(), programid, program, msg)
	//sqlString := "insert into errortbl(errortime,programid,programName,errormsg) values(GETDATE(),'" + programid + "','" + program + "','" + msg + "')"
	//_, inserterr := db.Exec(sqlString)
	if inserterr != nil {
		LogError(Panic, inserterr.Error())
	} else {
		LogError(Panic, program+" "+msg)
	}

}

//--------------------------------------------------------------------
// function to insert debug message into db
//--------------------------------------------------------------------
func ErrorNP(db *sql.DB, programid string, program string, msg string) {
	sqlString := "insert into errortbl(errortime,programid,programName,errormsg) values(?,?,?,?)"
	_, inserterr := db.Exec(sqlString, time.Now(), programid, program, msg)
	//sqlString := "insert into errortbl(errortime,programid,programName,errormsg) values(GETDATE(),'" + programid + "','" + program + "','" + msg + "')"
	//_, inserterr := db.Exec(sqlString)
	if inserterr != nil {
		LogError(NoPanic, inserterr.Error())
	}
}

//--------------------------------------------------------------------
// function to log error
//--------------------------------------------------------------------
func LogError(logtype string, msg interface{}) {
	log.Println(msg)
	if logtype == "P" {
		log.Print("Press 'Enter' to exit...")
		bufio.NewReader(os.Stdin).ReadBytes('\n')
		panic(msg)
	}
}

//--------------------------------------------------------------------
// function to get value from core setting for a given Key
//--------------------------------------------------------------------
func GetCoreSettingValue(db *sql.DB, key string) string {
	var value string
	sqlString := "select valuev from CoreSettings where keyv ='" + key + "'"
	rows, err := db.Query(sqlString)
	if err != nil {
		LogError("NP", err.Error())
	}
	for rows.Next() {

		err := rows.Scan(&value)
		if err != nil {
			LogError("NP", err)
		}

	}
	return value
}

//--------------------------------------------------------------------
// function return nil when the given string is blank or it will
// retrun the actual string
// main usage of this function is to avoid inserting/update 1900-01-01
// date for a date column in database for values that are blank
//--------------------------------------------------------------------

func ReturnNil(s string) interface{} {
	if s == "" {
		return nil
	} else {
		return s
	}
}

//--------------------------------------------------------------------
// function to get system hour
//--------------------------------------------------------------------
func GetCurrentHr() string {
	dt := time.Now()
	return dt.Format("15")
}

//--------------------------------------------------------------------
// function to record program start and end time details into db
//--------------------------------------------------------------------
func RecordRunDetails(db *sql.DB, id int, runType string, programName string, count int, cmt string) (int, error) {
	insertedID := 0
	if runType == INSERT {
		insertString := "INSERT INTO SchedulerRunDetails(StartTime,ProgramName,RecordCount,comment)  values(?,?,?,?)  "
		insertRes, inserterr := db.Exec(insertString, time.Now(), programName, count, cmt)

		if inserterr != nil {
			return insertedID, fmt.Errorf("Error while inserting SchedulerRunDetails: ", inserterr.Error())
			//LogError(Panic, inserterr.Error())
		} else {
			returnId, _ := insertRes.LastInsertId()
			insertedID = int(returnId)
		}
	} else if runType == UPDATE {
		insertedID = id
		updateString := "UPDATE SchedulerRunDetails  SET EndTime=?,RecordCount=?,comment=? where id=? "

		_, updateerr := db.Exec(updateString, time.Now(), count, cmt, insertedID)
		if updateerr != nil {
			//log.Println(updateerr.Error())
			return insertedID, fmt.Errorf("Error while updating SchedulerRunDetails: ", updateerr.Error())

		}
	}
	return insertedID, nil

}

//---------------------------------------------------------------------------------
//Function inserts and return upload master ID
//---------------------------------------------------------------------------------
func InsertUploadMaster(db *sql.DB, uploadMasterRec UploadMaster) (int, error) {
	var datetime = time.Now()
	insertString := "INSERT INTO UploadDataMaster (Summary,SourceCode,IsDefault,IsForceDefault,ZohoDepartmentId,UploadFileName,UploadedBy,UploadDate,CampaignId,IsCampaign) values(?,?,?,?,?,?,?,?,?,?) "
	insertedID := 0
	insertRes, inserterr := db.Exec(insertString, uploadMasterRec.Summary, uploadMasterRec.SourceCode, uploadMasterRec.IsDefault, uploadMasterRec.IsForceDefault, uploadMasterRec.ZohoDepartmentId, uploadMasterRec.UploadFileName, uploadMasterRec.UploadedBy, datetime, uploadMasterRec.CampaignId, uploadMasterRec.IsCampaign)
	if inserterr != nil {
		//log.Println(inserterr)
		return insertedID, fmt.Errorf("Error while inserting insertUploadMaster: ", inserterr.Error())

	} else {
		returnId, _ := insertRes.LastInsertId()
		insertedID = int(returnId)
	}
	return insertedID, nil
}

//---------------------------------------------------------------------------------
//Function inserts record into ticket log table
//---------------------------------------------------------------------------------
func InsertTicketLog(db *sql.DB, ticket TicketLog) error {
	var datetime = time.Now()
	insertString := "INSERT INTO ticketlog (ClientId,[Description],Source,Summary_User,TicketStatus,CreatedDate,ZohoDepartmentId,UploadDataMasterId,AssigneeId,STCode,CreatedBy,CreatedProgram,UpdatedBy,UpdatedDate,UpdatedProgram,isdeleted,Processed,Spawned) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
	_, inserterr := db.Exec(insertString, ticket.ClientId, ticket.Description, ticket.Source, ticket.SummaryUser, ticket.TicketStatus, datetime, ticket.ZohoDepartmentId, ticket.UploadDataMasterId, ReturnNil(ticket.AssigneeId), ticket.STCode, ticket.CreatedBy, ticket.CreatedProgram, ticket.UpdatedBy, datetime, ticket.UpdatedProgram, 0, 0, 0)
	if inserterr != nil {
		return fmt.Errorf("Error while inserting insertTicketLog: ", inserterr.Error())
		//log.Println(inserterr.Error())
	}
	return nil
}

//---------------------------------------------------------------------------------
//Function inserts record into ticket log table with return of id
//---------------------------------------------------------------------------------
func InsertTicketLog2(db *sql.DB, ticket TicketLog) (error, int) {
	var datetime = time.Now()
	insertString := "INSERT INTO ticketlog (ClientId,[Description],Source,Summary_User,TicketStatus,CreatedDate,ZohoDepartmentId,UploadDataMasterId,AssigneeId,STCode,CreatedBy,CreatedProgram,UpdatedBy,UpdatedDate,UpdatedProgram,isdeleted,Processed,Spawned) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)  "
	insertedID := 0
	insertRes, inserterr := db.Exec(insertString, ticket.ClientId, ticket.Description, ticket.Source, ticket.SummaryUser, ticket.TicketStatus, datetime, ticket.ZohoDepartmentId, ticket.UploadDataMasterId, ReturnNil(ticket.AssigneeId), ticket.STCode, ticket.CreatedBy, ticket.CreatedProgram, ticket.UpdatedBy, datetime, ticket.UpdatedProgram, 0, 0, 0)

	if inserterr != nil {
		return fmt.Errorf("Error while inserting insertTicketLog: ", inserterr.Error()), insertedID
		//log.Println(inserterr.Error())
	} else {
		returnId, _ := insertRes.LastInsertId()
		insertedID = int(returnId)
	}
	return nil, insertedID
}

//---------------------------------------------------------------------------------
//Function inserts record into ticket log table
//---------------------------------------------------------------------------------
func UpdateTicketLog(db *sql.DB, ticket TicketLog) error {
	var datetime = time.Now()
	updateString := "update TicketLog  set TicketStatus=?, AssigneeId=?, ZohoDepartmentId=?, stcode=?,UpdatedBy=?, UpdatedDate=?,UpdatedProgram=? where id=? "
	_, updateerr := db.Exec(updateString, ticket.TicketStatus, ReturnNil(ticket.AssigneeId), ticket.ZohoDepartmentId, ticket.STCode, ticket.UpdatedBy, datetime, ticket.UpdatedProgram, ticket.Id)
	if updateerr != nil {
		return fmt.Errorf("Error while updating UpdateTicketLog: ", updateerr.Error())
		//log.Println(updateerr.Error())
	}
	return nil
}

//---------------------------------------------------------------------------------
//Function inserts record into ticket log table with spawned, processed
//---------------------------------------------------------------------------------
func UpdateTicketLog2(db *sql.DB, ticket TicketLog) error {
	var datetime = time.Now()
	updateString := "update TicketLog  set TicketStatus=?, AssigneeId=?, ZohoDepartmentId=?, stcode=?,UpdatedBy=?, UpdatedDate=?,UpdatedProgram=?, spawned=$?, processed=$?,description=$?,Summary_User=$? where id=$? "
	_, updateerr := db.Exec(updateString, ticket.TicketStatus, ReturnNil(ticket.AssigneeId), ticket.ZohoDepartmentId, ticket.STCode, ticket.UpdatedBy, datetime, ticket.UpdatedProgram, ticket.Spawned, ticket.Processed, ticket.Description, ticket.SummaryUser, ticket.Id)
	if updateerr != nil {
		return fmt.Errorf("Error while updating UpdateTicketLog: ", updateerr.Error())
		//log.Println(updateerr.Error())
	}
	return nil
}

//---------------------------------------------------------------------------------
//Function to manage record for email log table
//---------------------------------------------------------------------------------
func EmailLog(db *sql.DB, email EmailLogType) error {
	var datetime = time.Now()
	if email.Action == INSERT {
		insertString := "INSERT INTO Emaillog (FromId,FromDspName,ToId,Cc,Bcc,Subject,Body,CreationDate,Status,CreatedProgram,EmailServer,ReplyTo) values(?,?,?,?,?,?,?,?,?,?,?,?)"
		_, inserterr := db.Exec(insertString, email.From, email.FromDspName, email.To, email.Cc, email.Bcc, email.Subject, email.Body, datetime, "NEW", email.CreatedProgram, email.EmailServer, email.ReplyTo)
		if inserterr != nil {
			return fmt.Errorf("Error while inserting EmailLog: ", inserterr.Error())
			//log.Println(inserterr.Error())
		}
	} else if email.Action == UPDATE {
		updateString := "update Emaillog  set SentDate=?,Status=?,ErrorMsg=? where id=? "
		_, updateerr := db.Exec(updateString, email.SentDate, email.Status, email.ErrorMsg, email.Id)
		if updateerr != nil {
			return fmt.Errorf("Error while updating EmailLog: ", updateerr.Error())
			//log.Println(updateerr.Error())
		}
	}
	return nil
}

// closeFile is a helper function which streamlines closing
// with error checking on different file types.
func closeFile(f myCloser) {
	err := f.Close()
	if err != nil {
		log.Println(err)
	}
}

//--------------------------------------------------------------------
// readCsv function reads csv []byte and convert them into a string array
//--------------------------------------------------------------------
func readCsvByte(file *zip.File) ([][]string, error) {
	log.Println("readCsvByte" + "(+)")
	var data [][]string
	fc, err := file.Open()
	defer closeFile(fc)
	if err != nil {
		log.Println("readCsvByte" + "(-)")
		return data, err
	} else {
		reader := csv.NewReader(fc)
		reader.Comma = '|'

		for {
			record, err := reader.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				// handle the error...
				// break? continue? neither?
			}
			data = append(data, record)
		}
	}
	log.Println("readCsvByte" + "(-)")
	return data, nil
}

//--------------------------------------------------------------------
// main function executed from command
//--------------------------------------------------------------------
func readCSVFromLocal(file string, delimitor rune) ([][]string, error) {
	var data [][]string
	log.Println("readCSVFromUrl" + "(+)")
	resp, err := os.Open(file)
	defer resp.Close()
	if err != nil {
		log.Println("readCSVFromUrl" + "(-)")
		return data, err
	}
	reader := csv.NewReader(resp)
	reader.Comma = delimitor
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			// handle the error...
			// break? continue? neither?
		}
		data = append(data, record)
	}
	log.Println("readCSVFromUrl" + "(-)")

	return data, nil
}

//--------------------------------------------------------------------
// main function executed from command
//--------------------------------------------------------------------
func readCSVFromUrl(url string, delimitor rune) ([][]string, error) {
	var data [][]string
	log.Println("readCSVFromUrl" + "(+)")
	resp, err := http.Get(url)
	if err != nil {
		log.Println("readCSVFromUrl" + "(-)")
		return data, err
	}
	defer resp.Body.Close()
	reader := csv.NewReader(resp.Body)
	reader.Comma = delimitor
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			// handle the error...
			// break? continue? neither?
		}
		data = append(data, record)
	}
	log.Println("readCSVFromUrl" + "(-)")

	return data, nil
}

//--------------------------------------------------------------------
// main function executed from command
//--------------------------------------------------------------------
func readCSVFromUrl2(url string, delimitor rune) ([][]string, error) {
	var data [][]string
	log.Println("readCSVFromUrl" + "(+)")

	client := http.DefaultClient
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		panic(err)
	}
	req.Header.Set("User-Agent", "PostmanRuntime/7.26.10")
	req.Header.Set("Accept", "*/*")
	req.Header.Set("Accept-Encoding", "gzip, deflate, br")
	req.Header.Set("Connection", "keep-alive")
	resp, err := client.Do(req)
	if err != nil {
		panic(err)
	}

	//resp, err := http.Get(url)
	if err != nil {
		log.Println("readCSVFromUrl" + "(-)")
		return data, err
	}
	defer resp.Body.Close()
	reader := csv.NewReader(resp.Body)
	reader.Comma = delimitor
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			// handle the error...
			// break? continue? neither?
		}
		//	log.Println(record)
		data = append(data, record)
	}
	log.Println("readCSVFromUrl" + "(-)")

	return data, nil
}
