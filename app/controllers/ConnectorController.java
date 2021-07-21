package controllers;

import com.avaje.ebean.Ebean;
import com.avaje.ebean.SqlRow;
import com.avaje.ebean.Transaction;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.sun.org.apache.xpath.internal.operations.Bool;
import org.quartz.JobExecutionContext;
import org.springframework.util.CollectionUtils;
import play.Logger;
import play.libs.Json;
import play.mvc.Controller;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;


/**
 * Created by @author Ashwini KR on 30-09-2020
 */
public class ConnectorController extends Controller implements org.quartz.Job {

    @Override
    public /*synchronized*/ void execute(JobExecutionContext context) {
        try {
            Logger.info("Inside execute() implementation Connector " + new Date());

            Long connectorId = null;

            for (Map.Entry<String, Object> entry : context.getJobDetail().getJobDataMap().entrySet()) {

                if (entry.getKey().equalsIgnoreCase("jobId")) {
                    connectorId = new Long(entry.getValue().toString());
                }
            }

            //System.out.println("IDDDDD - " + connectorId);
            //  System.out.println("Thread - " + Thread.currentThread() + "Connector - " + connectorId);

            String trackerStatusSql = "select status from bodhee.jdbc_job_tracker where jdbc_connector_id =  " + connectorId + "";
            SqlRow trackerStatus = Ebean.createSqlQuery(trackerStatusSql).findUnique();
            if (trackerStatus != null && trackerStatus.get("status") != null && !trackerStatus.get("status").toString().equalsIgnoreCase("running")) {
                String sqlJob = "select * from  bodhee.jdbc_connector where id = " + connectorId + "";

                SqlRow data = Ebean.createSqlQuery(sqlJob)
                        .setParameter("id", 1)
                        .findUnique();
                if (!CollectionUtils.isEmpty(data)) {

                    connectorLogic(data, connectorId);

                }
            }

            Logger.info("Out of execute() implementation  Connector" + new Date());
        } catch (Exception e) {
            Logger.debug("trace ", e);
            Logger.error("Function execute threw error " + e);
            e.printStackTrace();
            Logger.error("Exception : " + e);
        }
    }

    public /*synchronized*/ void connectorLogic(SqlRow sqlRow, Long connectorId) {

        try {
            // System.out.println("Thread - " + Thread.currentThread() + "Connector - " + connectorId);

            Date start = new Date();
            //System.out.println("CONN ID" + connectorId);
            //System.out.println("Start - " + start);
            //Fetch all the active data from connector table

            //for each connectors , check the source and destination

            if (sqlRow.get("ingestion_type_id") != null && sqlRow.getInteger("ingestion_type_id") == 1) {

                //fetching connectoInside executer data

                JsonNode columnMapping = (Json.parse(sqlRow.getString("column_mapping")));
                String sourceTableQuery = "select * from bodhee.jdbc_source_table where id = " + sqlRow.getInteger("source_table_id") + "";
                SqlRow sourceTableData = Ebean.createSqlQuery(sourceTableQuery).findUnique();
                String sourceSchema = sourceTableData.getString("schema");
                String sourceTable = sourceTableData.getString("table_name");
                String sourceDbQuery = "select * from bodhee.jdbc_db where id =  " + sourceTableData.getInteger("jdbc_db_id") + "";
                SqlRow sourceDbData = Ebean.createSqlQuery(sourceDbQuery).findUnique();
                String sourceServer = sourceDbData.getString("server");
                String sourceDriver = sourceDbData.getString("driver");
                String limitSql = "select value as limit from bodhee.jdbc_param_config where key = 'limit' ";
                SqlRow limitData = Ebean.createSqlQuery(limitSql).findUnique();
                Integer limit = limitData.get("limit") != null ? limitData.getInteger("limit") : 10000;


                //column mapping logic

                StringBuffer sourceCol = new StringBuffer();
                StringBuffer targetCol = new StringBuffer();
                Boolean first = true;
                JsonNode uniqueField = sqlRow.get("unique_column") == null ? Json.newObject() : Json.parse(sqlRow.getString("unique_column"));
                String targetUniqueField = uniqueField.get("target_col") == null ? "" : uniqueField.get("target_col").asText();
                String sourceUniqueField = uniqueField.get("source_col") == null ? "" : uniqueField.get("source_col").asText();

                if (targetUniqueField != null && sourceUniqueField != null && !targetUniqueField.equalsIgnoreCase("")
                        && !sourceUniqueField.equalsIgnoreCase("")) {
                    for (JsonNode jObj : columnMapping) {
                        if (jObj.get("source") != null && jObj.get("target") != null) {

                            if (first) {
                                first = false;
                            } else {
                                sourceCol.append(",");
                                targetCol.append(",");
                            }
                            sourceCol.append(jObj.get("source")).append(" as ").append(jObj.get("target"));
                            targetCol.append(jObj.get("target"));
                        }

                    }
                    //System.out.println("IDDDDD ConnLogic- " + connectorId);

                    //additional Params - Mandatory Params

                    targetCol.append(",node_id");
                    targetCol.append(",tenant_id");
                    targetCol.append(",result_cloud_sync");

                    //additional Params - Mandatory Params
                    StringBuffer sourceDataSql = new StringBuffer();
                    if (sourceDriver.contains("sqlserver")) {
                        sourceDataSql.append("select top ").append(limit).append(" ").append(sourceCol).append(" from ").append(sourceSchema).append(".").append(sourceTable).append(" where 1=1 ");

                    } else {
                        sourceDataSql.append("select ").append(sourceCol).append(" from ").append(sourceSchema).append(".").append(sourceTable).append(" where 1=1 ");
                    }
                    List<SqlRow> dataFromSource = null;
                    //fetching source data
                    if (!sqlRow.getBoolean("history_load")) {
                        //In oracle DB , if we have to add date condition we need to convert that into date type everytime.
                        //To do this we have to get data type of unique column

                        //                     Logic
                        //To identify the type of source unique column
                        //compare unique column in column mapping json and get the type

                        // ***IMP  :   Added above logic due to backward compatability and reduce configuration effort
                        //not making any changes in table or configuration. ****//

                        String sourceUniqueFieldDataType ="";

                        for (JsonNode jObj : columnMapping) {
                            if (jObj.get("source") != null && jObj.get("source").asText().equalsIgnoreCase(sourceUniqueField)) {
                                sourceUniqueFieldDataType =jObj.get("type").asText();
                            }
                        }

                        //log the error as  in column mapping , unique column is mandatory.
                        if(sourceUniqueFieldDataType.equalsIgnoreCase("")){
                            Logger.error("Unique column  is not added in the  column_mapping json of jdbc_connector table "  + new Date());
                            Logger.debug("Please add the Unique column in the  column_mapping json "  + new Date());
                        }else{
                            String currValSql = "select current_unique_value from bodhee.jdbc_job_tracker where jdbc_connector_id =  " + connectorId + "";
                            SqlRow currValData = Ebean.createSqlQuery(currValSql).findUnique();
                            if (currValData != null && currValData.get("current_unique_value") != null) {

                                String currVal = currValData.get("current_unique_value") == null ? "" : currValData.getString("current_unique_value");
                                if ( (sourceUniqueFieldDataType.equalsIgnoreCase("long") || sourceUniqueFieldDataType.equalsIgnoreCase("int")
                                        || sourceUniqueFieldDataType.equalsIgnoreCase("integer") || sourceUniqueFieldDataType.equalsIgnoreCase("numeric")
                                        || sourceUniqueFieldDataType.equalsIgnoreCase("float") || sourceUniqueFieldDataType.equalsIgnoreCase("double")
                                        || sourceUniqueFieldDataType.equalsIgnoreCase("double precision") )) {

                                    sourceDataSql.append(" and ").append(sourceUniqueField).append(" >  "+currVal+" ");

                                } else if((sourceUniqueFieldDataType.equalsIgnoreCase("date") || sourceUniqueFieldDataType.equalsIgnoreCase("timestamp")
                                        || sourceUniqueFieldDataType.equalsIgnoreCase("datetime"))){

                                    if(sourceDriver.contains("oracle")){
                                        sourceDataSql.append(" and ").append(sourceUniqueField).append(" >  ").append("  to_timestamp('").append(currVal).append("', 'yyyy-mm-dd hh24:mi:ss.ff') ");

                                    }else if (sourceDriver.contains("postgresql") || sourceDriver.contains("sqlserver") || sourceDriver.contains("mysql") ){
                                        sourceDataSql.append(" and ").append(sourceUniqueField).append(" > '").append(currVal).append("' ");
                                    }
                                }
                                sourceDataSql.append(" order by ").append(sourceUniqueField).append(" asc ");
                                if (sourceDriver.contains("postgresql") || sourceDriver.contains("mysql")) {
                                    sourceDataSql.append(" limit ").append(limit).append(" ");
                                }else if (sourceDriver.contains("oracle")){
                                    sourceDataSql.append(" FETCH FIRST ").append(limit).append(" ROWS ONLY ");
                                }
                                //Logger.info("No unique value found");
                                dataFromSource = Ebean.getServer(sourceServer).createSqlQuery(sourceDataSql.toString()).findList();
                                functionToCopy(dataFromSource, sqlRow, connectorId, targetCol, targetUniqueField, false);

                            } else {
                                Logger.info("No data from source");
                            }
                        }




                    } else if (sqlRow.getBoolean("history_load")) {
                        //history load

                        JsonNode historyCondition = sqlRow.get("last_extract_data") == null ? null : Json.parse(sqlRow.getString("last_extract_data"));
                        //find the history load condition and write the logic on it TODO

                        String andClause = appendAndClause(historyCondition,sourceDriver);

                        sourceDataSql.append(andClause);

                        sourceDataSql.append(" order by ").append(sourceUniqueField).append(" asc ");

                        if (sourceDriver.contains("postgresql") || sourceDriver.contains("mysql")) {
                            sourceDataSql.append(" limit ").append(limit).append(" ");

                        }else if (sourceDriver.contains("oracle")){
                            sourceDataSql.append(" FETCH FIRST ").append(limit).append(" ROWS ONLY ");
                        }
                        //System.out.println("Data  query " + sourceDataSql.toString());
                        dataFromSource = Ebean.getServer(sourceServer).createSqlQuery(sourceDataSql.toString()).findList();
                        //System.out.println("Data size " + dataFromSource.size());
                        if (!CollectionUtils.isEmpty(dataFromSource)) {
                            functionToCopy(dataFromSource, sqlRow, connectorId, targetCol, targetUniqueField, true);

                        }

                    }
                    //System.out.println(sourceDataSql.toString());

                    Date end = new Date();
                    //System.out.println("End - " + end);
                    updateExecutionTime(start, end, connectorId);

                } /*else if (sqlRow.get("ingestion_type_id") != null && sqlRow.getInteger("ingestion_type_id") == 2) {


            }*/

            }


        } catch (Exception e) {
            Logger.debug("trace ", e);
            Logger.error("Function connectorLogic() threw error " + e);
            e.printStackTrace();
        }

    }

    public /*synchronized*/ void functionToCopy(List<SqlRow> dataFromSource, SqlRow sqlRow, Long connectorId, StringBuffer targetCol, String targetUniqueField, Boolean histFlag) throws SQLException {
        Transaction transaction = null;
        transaction = Ebean.beginTransaction();
        Connection connection = transaction.getConnection();
        Statement stmt = connection.createStatement();
        //System.out.println("IDDDDD functionToCopy- " + connectorId);

        try {
            // System.out.println("Thread - " + Thread.currentThread() + "Connector - " + connectorId);

            updateStatus(connectorId, "RUNNING");

            JsonNode nodeMapping = sqlRow.get("node_mapping") == null ? Json.newObject() : Json.parse(sqlRow.getString("node_mapping"));
            Integer nodeId = 0, tenantId = 0;

            //fetching target data

            String targetTableQuery = "select * from bodhee.jdbc_target_table where  id = " + sqlRow.getInteger("source_table_id") + "";
            SqlRow targetTableData = Ebean.createSqlQuery(targetTableQuery).findUnique();
            String targetSchema = targetTableData.getString("schema");
            String targetTable = targetTableData.getString("table_name");
            String targetDbQuery = "select * from bodhee.jdbc_db where id =  " + targetTableData.getInteger("jdbc_db_id") + "";
            SqlRow targetDbData = Ebean.createSqlQuery(targetDbQuery).findUnique();
            String targetServer = targetDbData.getString("server");


            String batchSizeSql = "select value as batch_size from bodhee.jdbc_param_config where key = 'batch_size' ";
            SqlRow batchSizeData = Ebean.createSqlQuery(batchSizeSql).findUnique();
            Integer batchSize = batchSizeData.get("batch_size") != null ? batchSizeData.getInteger("batch_size") : 1000;


            tenantId = sqlRow.get("tenant_id") == null ? 0 : sqlRow.getInteger("tenant_id");
            // int size = dataFromSource.size();
            // int lastDataTracked = 0;


            if (!CollectionUtils.isEmpty(dataFromSource)) {

                Gson gson = new GsonBuilder().serializeNulls().create();
                String res = gson.toJson(dataFromSource);
                if (dataFromSource.get(0) != null && dataFromSource.get(0).get(targetUniqueField) != null) {
                    updateTracker(connectorId, "SUCCESS", dataFromSource.get(dataFromSource.size() - 1).get(targetUniqueField));

                    int dataCount = 0;


                    for (JsonNode jNode : Json.parse(res.replaceAll("'", ""))) {

                        //Node Mapping logic

                        if(sqlRow.get("node_id")!=null && Integer.parseInt(sqlRow.get("node_id").toString())!=0){
                            nodeId = (Integer) sqlRow.get("node_id");
                        }
                        else if (nodeMapping.size() > 0) {
                            //Node Mapping logic New
                            StringBuffer nodeSql = new StringBuffer("select node_id  from  bodhee.jdbc_node_mapping where 1=1");

                            for (JsonNode map : nodeMapping) {
                                String sourceColumn = map.get("source_column") == null ? "" : map.get("source_column").asText();
                                String mappingColumn = map.get("mapping_column") == null ? "" : map.get("mapping_column").asText();
                                String datatype = map.get("datatype") == null ? "" : map.get("datatype").asText();

                                if (datatype != null && datatype.equalsIgnoreCase("long") || datatype.equalsIgnoreCase("int") ||
                                        datatype.equalsIgnoreCase("integer") || datatype.equalsIgnoreCase("numeric")) {
                                    nodeSql.append(" and ").append(mappingColumn).append("=").append(jNode.get(sourceColumn).asText());

                                } else if (datatype != null && datatype.equalsIgnoreCase("string") || datatype.equalsIgnoreCase("date")) {
                                    nodeSql.append(" and ").append(mappingColumn).append("=").append("'").append(jNode.get(sourceColumn).asText()).append("'");

                                }
                            }

                            SqlRow nodeData = Ebean.createSqlQuery(nodeSql.toString()).findUnique();
                            if (nodeData != null && nodeData.get("node_id") != null) {
                                nodeId = nodeData.getInteger("node_id");
                            }


                            //Node Mapping logic New
                        }

                        //Node Mapping logic


                        ((ObjectNode) jNode).put("node_id", nodeId);
                        ((ObjectNode) jNode).put("tenant_id", tenantId);
                        ((ObjectNode) jNode).put("result_cloud_sync", false);

                        if (targetCol != null && targetSchema != null && targetTable != null && jNode != null && jNode.size() > 0 &&
                                !targetCol.toString().equalsIgnoreCase("") && !targetSchema.equalsIgnoreCase("") &&
                                !targetTable.equalsIgnoreCase("")) {


                            String sqlInsert = "INSERT INTO " + targetSchema + "." + targetTable + "( " +
                                    targetCol + " )" + "  " + "(select " + targetCol + " from  jsonb_populate_record(null::" +
                                    targetSchema + "." + targetTable + ",'" + jNode + "') )\n";

                            if (dataCount < batchSize) {


                                stmt.addBatch(sqlInsert);


                                //System.out.println("Batch ");
                                dataCount++;

                            }

                            //TODO : add batch execution logic

                            else {

                                if (stmt != null) {
                                    stmt.addBatch(sqlInsert);
                                    stmt.executeBatch();
                                    connection.commit();

                                    //System.out.println("Batch insert");
                                    dataCount = 0;
                                }


                            }

                        }

                        if (histFlag) {
                            String updateHistFlagSql = "update bodhee.jdbc_connector set history_load = false where id = " + connectorId + " ;commit";
                            Ebean.createSqlUpdate(updateHistFlagSql).execute();
                            histFlag = false;
                        }
                    }
                    if (stmt != null) {
                        stmt.executeBatch();
                        connection.commit();
                    }
                    updateStatus(connectorId, "SUCCESS");
                }


            }else{
                Logger.info(" No new data found for the table : "+targetTable+" "+ new Date());
                updateStatus(connectorId, "SUCCESS");

            }

        } catch (Exception e) {
            updateStatus(connectorId, "FAILED");
            Logger.debug("trace ", e);
            Logger.error("Function functionToCopy() threw error " + e);
            e.printStackTrace();
        } finally {
            stmt.close();
            transaction.end();
            connection.close();
        }

    }

    public /*synchronized*/ void updateExecutionTime(Date start, Date end, Long connectorId) {
        try {
            //System.out.println("IDDDDD updateExecutionTime- " + connectorId);
            //System.out.println("Thread - " + Thread.currentThread() + "Connector - " + connectorId);

            String execution = " update bodhee.jdbc_job_tracker set execution_time = " + TimeUnit.MILLISECONDS.toSeconds(Math.abs(start.getTime() - end.getTime())) + " where  jdbc_connector_id = " + connectorId + " ;commit ";
            Ebean.createSqlUpdate(execution).execute();

        } catch (Exception e) {
            Logger.debug("trace ", e);
            Logger.error("Function updateExecutionTime() threw error " + e);
            e.printStackTrace();
        }
    }

    public /*synchronized*/ void updateStatus(Long connectorId, String status) {
        try {
            //System.out.println("Thread - " + Thread.currentThread() + "Connector - " + connectorId);

            //System.out.println("IDDDDD updateStatus- " + connectorId);

            String execution = " update bodhee.jdbc_job_tracker set status = ? ,last_update_date = current_timestamp where  jdbc_connector_id = " + connectorId + ";commit ";
            Ebean.createSqlUpdate(execution).setParameter(1, status).execute();

        } catch (Exception e) {
            Logger.debug("trace ", e);
            Logger.error("Function updateStatus() threw error " + e);
            e.printStackTrace();
        }
    }

    public /*synchronized*/ void updateTracker(Long connectorId, String status, Object curVal) {
        try {
            // System.out.println("Thread - " + Thread.currentThread() + "Connector - " + connectorId);

            String sql = "select count(1) from bodhee.jdbc_job_tracker where jdbc_connector_id = " + connectorId + "";

            SqlRow data = Ebean.createSqlQuery(sql).findUnique();
            if (data != null && data.get("count") != null && Integer.parseInt(data.get("count").toString()) > 0) {
                String execution = " update bodhee.jdbc_job_tracker set status = ? ,last_update_date = current_timestamp , current_unique_value = ? " +
                        ",current_run_date = current_timestamp  where  jdbc_connector_id = " + connectorId + ";commit ";
                Ebean.createSqlUpdate(execution).setParameter(1, status).setParameter(2, curVal.toString()).execute();

            } else {
                String insertSql = "INSERT INTO bodhee.jdbc_job_tracker(\n" +
                        "             jdbc_connector_id, status, current_run_date, current_unique_value, \n" +
                        "            creation_date, last_update_date)\n" +
                        "    VALUES (  ?, ?, ?, ?, \n" +
                        "            ?, ?); commit; ";
                Ebean.createSqlUpdate(insertSql)
                        .setParameter(1, connectorId)
                        .setParameter(2, status)
                        .setParameter(3, new Date())
                        .setParameter(4, curVal)
                        .setParameter(5, new Date())
                        .setParameter(6, new Date())
                        .execute();

            }


        } catch (Exception e) {
            Logger.debug("trace ", e);
            Logger.error("Function updateTracker() threw error " + e);
            e.printStackTrace();
        }
    }

    public String appendAndClause(JsonNode historyCondition,String sourceDriver) {
        StringBuffer sourceDataSql = new StringBuffer();
        if (historyCondition != null) {

            for (JsonNode jNode : historyCondition) {

                sourceDataSql.append(" and ").append(jNode.get("key").asText()).append(" ").append(jNode.get("operator").asText())
                        .append(" ");

                if (jNode.get("datatype") != null && jNode.get("value") != null && (jNode.get("datatype").asText().equalsIgnoreCase("long") || jNode.get("datatype").asText().equalsIgnoreCase("int") ||
                        jNode.get("datatype").asText().equalsIgnoreCase("integer") || jNode.get("datatype").asText().equalsIgnoreCase("numeric"))) {

                    sourceDataSql.append(jNode.get("value").asText());

                } else if(jNode.get("datatype") != null && jNode.get("value") != null && jNode.get("datatype").asText().equalsIgnoreCase("string")){

                    sourceDataSql.append("'").append(jNode.get("value").asText()).append("'");

                } else if (jNode.get("datatype") != null && jNode.get("value") != null && (jNode.get("datatype").asText().equalsIgnoreCase("date")
                        || jNode.get("datatype").asText().equalsIgnoreCase("datetime") || jNode.get("datatype").asText().equalsIgnoreCase("timestamp") )) {

                    if(sourceDriver.contains("oracle")){

                        sourceDataSql.append(" DATE  '").append(jNode.get("value").asText()).append("' ");

                    }else if (sourceDriver.contains("postgresql") || sourceDriver.contains("sqlserver") || sourceDriver.contains("mysql") ){

                        sourceDataSql.append("'").append(jNode.get("value").asText()).append("'");
                    }

                }
            }
        }
        return sourceDataSql.toString();
    }
}
