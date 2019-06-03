package com.infosys.juniper.dao;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.springframework.stereotype.Component;

import com.infosys.juniper.constant.MetadataDBConstants;
import com.infosys.juniper.dto.FeedDto;


@Component
public class JuniperOnPremExtractFeedDaoImpl implements JuniperOnPremExtractFeedDao {

	@Override
	public String insertFeedDetails(Connection conn, FeedDto feedDto) {
		int project_sequence=0;
		String insertFeedMaster="";
		String insertSrcTgtLink="";
		String feed_id="";
		String sequence="";

		try {
			project_sequence=getProjectSequence(conn,feedDto.getProject());

		}catch(SQLException e) {
			e.printStackTrace();
			return "Error while retrieving Project details";
		}

		if(project_sequence!=0) {

			insertFeedMaster=MetadataDBConstants.INSERTQUERY.replace("{$table}", MetadataDBConstants.FEEDTABLE)
					.replace("{$columns}", "feed_unique_name,country_code,extraction_type,project_sequence,created_by")
					.replace("{$data}", MetadataDBConstants.QUOTE +feedDto.getFeed_name() +MetadataDBConstants.QUOTE+MetadataDBConstants.COMMA
							+MetadataDBConstants.QUOTE+feedDto.getCountry_code()+MetadataDBConstants.QUOTE+MetadataDBConstants.COMMA
							+MetadataDBConstants.QUOTE+feedDto.getFeed_extract_type()+MetadataDBConstants.QUOTE+MetadataDBConstants.COMMA
							+project_sequence+MetadataDBConstants.COMMA
							+MetadataDBConstants.QUOTE+feedDto.getJuniper_user()+MetadataDBConstants.QUOTE
							);

			try {	
				Statement statement = conn.createStatement();
				statement.execute(insertFeedMaster);
				String query=MetadataDBConstants.GETSEQUENCEID.replace("${tableName}", MetadataDBConstants.FEEDTABLE).replace("${columnName}", MetadataDBConstants.FEEDTABLEKEY);
				ResultSet rs=statement.executeQuery(query);
				if(rs.isBeforeFirst()) {
					rs.next();
					sequence=rs.getString(1).split("\\.")[1];
					rs=statement.executeQuery(MetadataDBConstants.GETLASTROWID.replace("${id}", sequence));
					if(rs.isBeforeFirst()) {
						rs.next();
						feed_id=rs.getString(1);
						for(String target:feedDto.getTarget().split(",")) {
							insertSrcTgtLink=MetadataDBConstants.INSERTQUERY.replace("{$table}", MetadataDBConstants.FEEDSRCTGTLINKTABLE)
									.replace("{$columns}", "feed_sequence,src_conn_sequence,target_sequence,created_by")
									.replace("{$data}", feed_id+MetadataDBConstants.COMMA
											+feedDto.getSrc_conn_id()+MetadataDBConstants.COMMA
											+target+MetadataDBConstants.COMMA
											+MetadataDBConstants.QUOTE+feedDto.getJuniper_user()+MetadataDBConstants.QUOTE
											);
							statement.execute(insertSrcTgtLink);
						}
						
					}
				}
			}catch (SQLException e) {

				e.printStackTrace();
				return(e.getMessage());


			}finally {
				try {
					conn.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					return "Error while closing connection";
				}
			}
		}else {

			return "Project details are invalid";
		}

		return "Success:"+feed_id;
	}
	
	
	private int getProjectSequence(Connection conn, String project) throws SQLException {
		// TODO Auto-generated method stub
		String query="select project_sequence from "+MetadataDBConstants.PROJECTTABLE+" where project_id='"+project+"'";
		int proj_seq=0;
		Statement statement=conn.createStatement();
		ResultSet rs = statement.executeQuery(query);
		if(rs.isBeforeFirst()) {

			rs.next();
			proj_seq=rs.getInt(1);


		}
		
		return proj_seq;
	}


	@Override
	public String updateFeedDetails(Connection conn, FeedDto feedDto) {
		int project_sequence=0;
		String updateFeedMaster="";
		String insertSrcTgtLink="";
		String deleteSrcTgtLink="";
		try {
			project_sequence=getProjectSequence(conn,feedDto.getProject());

		}catch(SQLException e) {
			e.printStackTrace();
			return "Error while retrieving Project details";
		}

		if(project_sequence!=0) {


			updateFeedMaster="update "+MetadataDBConstants.FEEDTABLE 
					+" set extraction_type="+MetadataDBConstants.QUOTE+feedDto.getFeed_extract_type()+MetadataDBConstants.QUOTE+MetadataDBConstants.COMMA
					+"feed_unique_name="+MetadataDBConstants.QUOTE+feedDto.getFeed_name()+MetadataDBConstants.QUOTE+MetadataDBConstants.COMMA
					+"country_code="+MetadataDBConstants.QUOTE+feedDto.getCountry_code()+MetadataDBConstants.QUOTE+MetadataDBConstants.COMMA
					+"project_sequence="+project_sequence+MetadataDBConstants.COMMA
					+"updated_by="+MetadataDBConstants.QUOTE+feedDto.getJuniper_user()+MetadataDBConstants.QUOTE
					+" where feed_sequence="+feedDto.getFeed_id();
			
			deleteSrcTgtLink="delete from "+MetadataDBConstants.FEEDSRCTGTLINKTABLE
					+" where feed_sequence="+feedDto.getFeed_id();
			
			for(String target:feedDto.getTarget().split(",")) {
				insertSrcTgtLink=MetadataDBConstants.INSERTQUERY.replace("{$table}", MetadataDBConstants.FEEDSRCTGTLINKTABLE)
						.replace("{$columns}", "feed_sequence,src_conn_sequence,target_sequence,created_by")
						.replace("{$data}", feedDto.getFeed_id()+MetadataDBConstants.COMMA
								+feedDto.getSrc_conn_id()+MetadataDBConstants.COMMA
								+target+MetadataDBConstants.COMMA
								+MetadataDBConstants.QUOTE+feedDto.getJuniper_user()+MetadataDBConstants.QUOTE
								);
			}
			
			
			
			try {	
				Statement statement = conn.createStatement();
				statement.execute(updateFeedMaster);
				statement.executeQuery(deleteSrcTgtLink);
				statement.execute(insertSrcTgtLink);
				return "Success";

			}catch (SQLException e) {
				return e.getMessage();


			}finally {
				try {
					conn.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					return "Error while closing connection";
				}
			}

		}
		else {
			return "Project Details are invalid";

		}

	}


	@Override
	public String deleteFeedDetails(Connection conn, FeedDto feedDto) {
		String deleteFeedMaster="delete from "+MetadataDBConstants.FEEDTABLE+" where feed_sequence="+feedDto.getFeed_id();
		String deleteSrcTgtLink="delete from "+MetadataDBConstants.FEEDSRCTGTLINKTABLE+" where feed_sequence="+feedDto.getFeed_id();

		try {	
			Statement statement = conn.createStatement();
			statement.executeQuery(deleteSrcTgtLink);
			statement.execute(deleteFeedMaster);
			return "Success";

		}catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			//TODO: Log the error message
			return e.getMessage();


		}finally {
			try {
				conn.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return "Error while closing connection";
			}
		}
	}

}
