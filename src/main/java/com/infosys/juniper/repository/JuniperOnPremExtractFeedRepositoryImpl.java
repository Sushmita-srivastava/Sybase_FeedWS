package com.infosys.juniper.repository;

import java.sql.Connection;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.infosys.juniper.dao.JuniperOnPremExtractFeedDao;
import com.infosys.juniper.dto.FeedDto;
import com.infosys.juniper.util.MetadataDBConnectionUtils;


@Component
public class JuniperOnPremExtractFeedRepositoryImpl implements JuniperOnPremExtractFeedRepository {

	@Autowired
	JuniperOnPremExtractFeedDao Dao;
	
	
	@Override
	public String onboardSystem(FeedDto feedDto) {
		Connection conn=null;
		try {
			conn=MetadataDBConnectionUtils.getOracleConnection();
			
		}catch(Exception e) {
			e.printStackTrace();
			return "Failed to connect to Metadata database";
		}
		
		System.out.println("connection established to Metadata DB");
		return Dao.insertFeedDetails(conn, feedDto);
	}


	@Override
	public String updateFeed(FeedDto feedDto) {
		Connection conn=null;
		try {
			conn=MetadataDBConnectionUtils.getOracleConnection();
			
		}catch(Exception e) {
			e.printStackTrace();
			return "Failed to connect to Metadata database";
		}
		
		System.out.println("connection established to Metadata DB");
		return Dao.updateFeedDetails(conn, feedDto);
	}


	@Override
	public String deleteFeed(FeedDto feedDto) {
		Connection conn=null;
		try {
			conn=MetadataDBConnectionUtils.getOracleConnection();
			
		}catch(Exception e) {
			e.printStackTrace();
			return "Failed to connect to Metadata database";
		}
		
		System.out.println("connection established to Metadata DB");
		return Dao.deleteFeedDetails(conn, feedDto);
	}

}
