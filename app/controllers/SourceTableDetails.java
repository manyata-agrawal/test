package controllers;

import com.avaje.ebean.Ebean;
import com.avaje.ebean.SqlRow;
import com.avaje.ebean.annotation.Sql;
import play.Logger;

import java.util.Date;
import java.util.List;

/**
 * Created by Admin on 28-12-2020.
 */
public class SourceTableDetails {

    public void getSourceTableDetails(){

        //connect to source tables and run respective query and update in the postgres source table.

        try {


            String sql = "select query,server,b.id as id,query_parameter " +
                    " from  bodhee.jdbc_db a join jdbc_source_table b on   a.id = b.jdbc_db_id\n" +
                    " order by b.description; ";

            List<SqlRow> data = Ebean.createSqlQuery(sql)
                    .setParameter("id", 1)
                    .findList();

            for (SqlRow eachTableData : data) {

                String checkQuery = eachTableData.getString("query");
                String queryParameter = eachTableData.getString("query_parameter");
                String sourceServer = eachTableData.getString("server");
                String updateRowId = eachTableData.getString("id");

                SqlRow dataFromSource = Ebean.getServer(sourceServer).createSqlQuery(checkQuery.toString()).findUnique();

                long count = dataFromSource.getLong("count");

                String updateSourceTable = "update bodhee.jdbc_source_table " +
                        " set todays_count_in_source="+count+",last_updated_timestamp=current_timestamp" +
                        " where id=" + updateRowId + ";";

                Ebean.createSqlUpdate(updateSourceTable);

            }

        }catch (Exception e){
            Logger.debug("Exception occurred in getSourceTableDetails method"+new Date());
            e.printStackTrace();

        }

    }
}

