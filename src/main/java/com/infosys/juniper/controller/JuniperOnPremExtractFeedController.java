package com.infosys.juniper.controller;

import java.sql.SQLException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.infosys.juniper.dto.FeedDto;
import com.infosys.juniper.dto.RequestDto;
import com.infosys.juniper.repository.JuniperOnPremExtractFeedRepository;
import com.infosys.juniper.util.ResponseUtil;

@CrossOrigin
@RestController
public class JuniperOnPremExtractFeedController {
	
@Autowired
JuniperOnPremExtractFeedRepository Repository;


	@RequestMapping(value = "/onboardSystem", method = RequestMethod.POST, consumes = "application/json")
	@ResponseBody
	public String onBoardSystem(@RequestBody RequestDto requestDto) throws SQLException {
		String response="";
		String status="";
		String message="";

		FeedDto feedDto = new FeedDto();
		feedDto.setFeed_name(requestDto.getBody().get("data").get("feed_name"));
		feedDto.setSrc_conn_id(Integer.parseInt(requestDto.getBody().get("data").get("src_connection_id")));
		feedDto.setFeed_desc(requestDto.getBody().get("data").get("feed_desc"));
		feedDto.setCountry_code(requestDto.getBody().get("data").get("country_code"));
		feedDto.setFeed_extract_type(requestDto.getBody().get("data").get("feed_extract_type"));
		feedDto.setTarget(requestDto.getBody().get("data").get("target"));
		feedDto.setJuniper_user(requestDto.getBody().get("data").get("user"));
		feedDto.setProject(requestDto.getBody().get("data").get("project"));

		response=Repository.onboardSystem(feedDto);
		if(response.toLowerCase().contains("success")) {
			status="Success";
			message="Feed created with Feed Id-"+response.split(":")[1];
		}
		else {
			status="Failed";
			message=response;
		}
		return ResponseUtil.createResponse(status, message);
	}


	@RequestMapping(value = "/updSystem", method = RequestMethod.POST, consumes = "application/json")
	@ResponseBody
	public String updSystem(@RequestBody RequestDto requestDto) throws SQLException{
		String response="";
		String status="";
		String message="";
		FeedDto feedDto = new FeedDto();

		feedDto.setFeed_name(requestDto.getBody().get("data").get("feed_name"));
		feedDto.setSrc_conn_id(Integer.parseInt(requestDto.getBody().get("data").get("src_connection_id")));
		feedDto.setFeed_desc(requestDto.getBody().get("data").get("feed_desc"));
		feedDto.setCountry_code(requestDto.getBody().get("data").get("country_code"));
		feedDto.setFeed_extract_type(requestDto.getBody().get("data").get("feed_extract_type"));
		feedDto.setTarget(requestDto.getBody().get("data").get("target"));
		feedDto.setFeed_id(Integer.parseInt(requestDto.getBody().get("data").get("src_sys")));
		feedDto.setJuniper_user(requestDto.getBody().get("data").get("user"));
		feedDto.setProject(requestDto.getBody().get("data").get("project"));
		response=Repository.updateFeed(feedDto);
		if(response.equalsIgnoreCase("success")) {
			status="Success";
			message="Feed updated";
		}
		else {
			status="Failed";
			message=response;
		}


		return ResponseUtil.createResponse(status, message);
	}

	@RequestMapping(value = "/delSystem", method = RequestMethod.POST, consumes = "application/json")
	@ResponseBody
	public String delSystem(@RequestBody RequestDto requestDto) {
		String response="";
		String status="";
		String message="";
		FeedDto feedDto = new FeedDto();

		feedDto.setFeed_id(Integer.parseInt(requestDto.getBody().get("data").get("src_sys")));
		response=Repository.deleteFeed(feedDto);


			if(response.equalsIgnoreCase("success")) {
				status="Success";
				message="Feed deleted";
			}
			else {
				status="Failed";
				message=response;
			}

		

		// Parse Json to Dto Object
		return ResponseUtil.createResponse(status, message);
	}


}
