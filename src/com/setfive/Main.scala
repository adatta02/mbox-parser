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

case class ParsedEmail(from: String, subject: String, date: String, 
					   labels: List[String], toList: List[String], pk_id: Option[Integer] = None)

object Main {
  
  val mysqlConn = DriverManager.getConnection("jdbc:mysql://localhost/howrse?user=root&password=root")
  
  def main(args: Array[String]): Unit = {    
    val mboxFile = new FileInputStream( new File("/home/ashish/Downloads/myinbox_small.mbox") )    
    val reader = new BufferedReader( new InputStreamReader(mboxFile) )
    val iterator = Iterator.continually(reader.readLine()).takeWhile( _!= null )    
    
    val parsedEmails = iterator
	    .foldLeft( List[String]() )((list, el) => {    	
	    	if( "(?s).*From \\d+@xxx \\w+ \\w+ \\d+ \\d+:\\d+:\\d+ \\d+".r.findFirstIn(el) == None ){ 
	    	  list.dropRight(1) :+ ( list.last + "\n" + el )
	    	}else{
	    		list :+ el
	    	}						 
	    })
	    .map(extractEmailInfo)
	    .map(el => {
	      val stmt = mysqlConn.prepareStatement( "INSERT INTO email (created_at, email_from, subject) VALUES (?, ?, ?)" )
	      
	      stmt.setString(1, el.date)
	      stmt.setString(2, el.from)
	      stmt.setString(3, el.subject)
	      stmt.executeUpdate()	      
	      	      
	      val rs = stmt.executeQuery("SELECT LAST_INSERT_ID()");
	      val pk = if(rs.next()){ 
	        rs.getInt(1) 
	      }else{
	        throw new Exception("No auto increment ID?")
	      }
	      
	      el.copy( pk_id = Option[Integer](pk) )
	    })
	    
	println(parsedEmails)
	    
  }

  def extractEmailInfo(el:String): ParsedEmail = {
    
    val dateParser = new java.text.SimpleDateFormat("EEE, d MMM yyyy HH:mm:ss")
	val dateOutput = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    val labels = "(?s).*X-Gmail-Labels: ([\\w,]+)"
      			.r.findAllIn(el).matchData.map( _.group(1) ).toList

    val date = """(?s).*Date: ([\w\d\s,:]+)""".r.findFirstMatchIn(el) match {
	    case Some(m) => {	    	    
	    	try{
	    		dateOutput.format( dateParser.parse(m.group(1).trim()) )
	    	}catch {
	    		case e: Exception => ""
	    	}	    	    	    	    
	    }
	    case None => ""
    }

    val from = """(?s).*From: .*? <(.*?)>""".r.findFirstMatchIn(el) match {
    	case Some(m) => m.group(1).trim()
    	case None => ""
    }

    val subject = """(?s).*Subject: (.*?)(?m)^""".r.findFirstMatchIn(el) match {
    	case Some(m) => m.group(1).trim()
    	case None => ""
    }    
    
    val toBlock = """(?s).*To: (.*?)(?m)^""".r.findFirstMatchIn(el) match {
    	case Some(m) => m.group(1).trim()
    	case None => ""
    }

    val addressList = toBlock.split(",").map(el => {
    	"""(?s).*<(.*?)>""".r.findAllIn(el).matchData
    	.map( _.group(1).trim )
    	.toList.mkString("")
    }).toList
    
    new ParsedEmail(labels = labels, date = date, subject = subject, from = from, toList = addressList)	    	
  	}
  
}