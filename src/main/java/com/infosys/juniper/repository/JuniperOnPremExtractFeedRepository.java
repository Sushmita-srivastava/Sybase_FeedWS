package com.infosys.juniper.repository;

import com.infosys.juniper.dto.FeedDto;

public interface JuniperOnPremExtractFeedRepository {

	String onboardSystem(FeedDto feedDto);

	String updateFeed(FeedDto feedDto);

	String deleteFeed(FeedDto feedDto);
	
	
	

}
