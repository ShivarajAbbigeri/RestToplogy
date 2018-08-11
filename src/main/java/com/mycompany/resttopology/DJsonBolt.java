/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.resttopology;

import com.google.gson.Gson;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.task.TopologyContext;


public class DJsonBolt implements IRichBolt {
   private OutputCollector collector;
   Gson gson; 
   Connection conn = null;
   PreparedStatement stmt = null;
   @Override
   public void prepare(Map stormConf, TopologyContext context,
      OutputCollector collector) {
      this.collector = collector;
      gson = new Gson();
      try{
          
      Class.forName("com.mysql.jdbc.Driver");

      conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/sample","root","root");

      String sql = "insert into todo values(?,?,?,?)";
      stmt = conn.prepareStatement(sql);

   }catch(SQLException se){
      //Handle errors for JDBC
      se.printStackTrace();
   }catch(Exception e){
      //Handle errors for Class.forName
      e.printStackTrace();
   }//end try
   }
   
   @Override
   public void execute(Tuple input) {
      String JsonString = input.getString(0);
       
      Todo[] todoArray = gson.fromJson(JsonString, Todo[].class);
      
      for(Todo t: todoArray) {
          
          try {
              stmt.setInt(1, t.userId);
              stmt.setInt(2, t.id);
              stmt.setString(3, t.title);
              stmt.setBoolean(4, t.completed);
              stmt.executeUpdate();
          } catch (SQLException ex) {
              Logger.getLogger(DJsonBolt.class.getName()).log(Level.SEVERE, null, ex);
          }

      }

      collector.ack(input);
   }
   
   @Override
   public void declareOutputFields(OutputFieldsDeclarer declarer) {
   }

   @Override
   public void cleanup() {
   
       try {
           stmt.close();
           conn.close();
       } catch (SQLException ex) {
           Logger.getLogger(DJsonBolt.class.getName()).log(Level.SEVERE, null, ex);
       }
   }
   
   @Override
   public Map<String, Object> getComponentConfiguration() {
      return null;
   }
   
}


