package com.gwg.demo.mq.message;

import java.util.Date;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class UserMessage{
	
	 int id;
	 String name;
	 Date birthday;

}
