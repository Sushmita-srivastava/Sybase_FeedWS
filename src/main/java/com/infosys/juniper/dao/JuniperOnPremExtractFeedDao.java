package com.infosys.juniper.dao;

import java.sql.Connection;

import com.infosys.juniper.dto.FeedDto;

public interface JuniperOnPremExtractFeedDao {

	String insertFeedDetails(Connection conn, FeedDto feedDto);

	String updateFeedDetails(Connection conn, FeedDto feedDto);

	String deleteFeedDetails(Connection conn, FeedDto feedDto);
	
	
	

}
