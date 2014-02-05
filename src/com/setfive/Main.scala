package com.setfive

import java.io.File
import java.io.FileInputStream
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Date
import java.util.regex.Pattern
import java.sql.Connection
import java.sql.DriverManager
import java.sql.SQLException
import scala.util.{Try, Success, Failure}
import scala.util.matching.Regex

case class ParsedEmail(from: String, fromUser: String, fromDomain: String, subject: String, date: String, 
					   labels: List[String], toList: List[String], pk_id: Option[Integer] = None)

object Main {
  
  val mysqlConn = DriverManager.getConnection("jdbc:mysql://localhost/gmail?user=root&password=root&rewriteBatchedStatements=true")
  
  val dateParser = new java.text.SimpleDateFormat("EEE, d MMM yyyy HH:mm:ss")
  val dateOutput = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")  
  
  val labelsRe = "(?s).*X-Gmail-Labels: ([\\w,]+)".r
  val dateRe = """(?s).*Date: ([\w\d\s,:]+)""".r
  val fromRe = """(?s).*From: .*? <(.*?)>""".r
  val subjectRe = """(?s).*Subject: (.*?)(?m)^""".r
  val toRe = """(?s).*To: (.*?)(?m)^""".r
  val addressesRe = """(?s).*<(.*?)>""".r
  
  def main(args: Array[String]): Unit = {    
    
    if( args.length < 1 ){
      println("Sorry! You need to pass a filename to parse.")
      return
    }
    
    println("Opening mbox...")
    
    val mboxIiterator = new MboxIterator(args(0))    
        
    val parsedEmails = mboxIiterator
	    				.map(extractEmailInfo)
	    				.filter( e => e.labels.contains("Chat") == false )
	    				.map(insertEmailRow)
	    				.map(insertEmailMetadataRow)
	    	    
	println("Processed: " + parsedEmails.length + " emails.")
    
  }

  def insertEmailMetadataRow(el: ParsedEmail): ParsedEmail = {
    
	  this.mysqlConn.setAutoCommit(false)	      
	  val labelStmt =  this.mysqlConn.prepareStatement( "INSERT INTO email_label (email_id, label) " + "VALUES (?, ?)" )
	  val toStmt = this.mysqlConn.prepareStatement( "INSERT INTO email_to (email_id, email_to) " + "VALUES (?, ?)" )
	  
	  println("Inserting meta for " + el.subject)
	  
	  for(label <- el.labels){
	    labelStmt.setInt(1, el.pk_id.get)
	    labelStmt.setString(2, label)
	    labelStmt.addBatch()
	  }
	  
	  for(toAddr <- el.toList){
	    toStmt.setInt(1, el.pk_id.get)
	    toStmt.setString(2, toAddr)
	    toStmt.addBatch()
	  }
	  	      
	  labelStmt.executeBatch()	      
	  toStmt.executeBatch()
	  this.mysqlConn.commit()
	  
	  el
  }
  
  def insertEmailRow(el: ParsedEmail): ParsedEmail = {
        
	  this.mysqlConn.setAutoCommit(true)
	  val stmt = this.mysqlConn
			  		 .prepareStatement( "INSERT INTO email (created_at, email_from, subject, email_from_user, email_from_domain) " 
			  							+ "VALUES (?, ?, ?, ?, ?)" )
	  
	  println("Inserting " + el.subject)
			  						
	  stmt.setString(1, el.date)
	  stmt.setString(2, el.from)
	  stmt.setString(3, el.subject)
	  stmt.setString(4, el.fromUser)
	  stmt.setString(5, el.fromDomain)
	  
	  stmt.executeUpdate()	      
	  	      
	  val rs = stmt.executeQuery("SELECT LAST_INSERT_ID()");
	  val pk = if(rs.next()){ 
	    rs.getInt(1) 
	  }else{
	    throw new Exception("No auto increment ID?")
	  }
	  
	  el.copy( pk_id = Option[Integer](pk) )
  }
  
  def extractEmailInfo(el:String): ParsedEmail = {    
    
    val labels = this.labelsRe.findFirstMatchIn(el) match {
    	case Some(m) => m.group(1).split(",").toList
    	case None => List[String]()
    }    

    val date = this.dateRe.findFirstMatchIn(el) match {
	    case Some(m) => {	    	    
	    	try{
	    		this.dateOutput.format( this.dateParser.parse(m.group(1).trim()) )
	    	}catch {
	    		case e: Exception => ""
	    	}	    	    	    	    
	    }
	    case None => ""
    }
    
        
    val from = this.fromRe.findFirstMatchIn(el) match {
    	case Some(m) => m.group(1).trim()
    	case None => ""
    }

    val fromParts = if (from.indexOf("@") > 0) { from.split("@") } else { Array[String]("", "") }
    
    val subject = this.subjectRe.findFirstMatchIn(el) match {
    	case Some(m) => m.group(1).trim()
    	case None => ""
    }    
    
    val toBlock = this.toRe.findFirstMatchIn(el) match {
    	case Some(m) => m.group(1).trim()
    	case None => ""
    }

    val addressList = toBlock.split(",").map(el => {
    	this.addressesRe.findAllIn(el).matchData
    	.map( _.group(1).trim )
    	.toList.mkString("")
    }).toList
    
    println( el.split("\n")(0) )
    
    new ParsedEmail(labels = labels, date = date, fromUser = fromParts(0), fromDomain = fromParts(1), 
        			subject = subject, from = from, toList = addressList)	    	
  }
  
}