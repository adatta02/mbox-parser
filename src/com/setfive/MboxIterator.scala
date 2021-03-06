package com.setfive

import java.io.File
import java.io.FileInputStream
import java.io.BufferedReader;
import java.io.InputStreamReader;
import scala.util.Random

class MboxIterator(mboxFilename: String) extends Iterator[String] {
  
  val mboxFile = new FileInputStream( new File(mboxFilename) )    
  val reader = new BufferedReader( new InputStreamReader(mboxFile) )  
  val iterator = Iterator.continually(reader.readLine()).takeWhile( _!= null )
  
  val findRe = "(?s).*From \\d+@xxx \\w+ \\w+ \\d+ \\d+:\\d+:\\d+ \\d+".r
  var lastLine = ""
  
  def hasNext: Boolean = {
    reader.ready()
  }
  
  def next(): String = {
        
    var emailBody = this.lastLine
    var hasContentType = false
    
    for(line <- iterator){      
      
      if( line.indexOf("From") > -1 && line.matches( this.findRe.toString ) ){
        
        if( emailBody.length() == 0 ){
          emailBody = emailBody + line + "\n"
        }else{
          this.lastLine = line + "\n"
          return emailBody
        }
        
      }else{        
        if(!hasContentType) {
        	emailBody = emailBody + line + "\n"        	
        }
        hasContentType = hasContentType || line.indexOf("Content-Type:") == 0
      }
      
    }   
    
    emailBody
  } 
  
}