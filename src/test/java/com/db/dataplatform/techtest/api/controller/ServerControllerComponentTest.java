package com.db.dataplatform.techtest.api.controller;

import com.db.dataplatform.techtest.TestDataHelper;
import com.db.dataplatform.techtest.server.api.controller.ServerController;
import com.db.dataplatform.techtest.server.api.model.DataEnvelope;
import com.db.dataplatform.techtest.server.exception.HadoopClientException;
import com.db.dataplatform.techtest.server.component.Server;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.http.MediaType;
import org.springframework.http.converter.json.Jackson2ObjectMapperBuilder;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import java.util.ArrayList;
import java.util.List;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.springframework.test.web.servlet.setup.MockMvcBuilders.standaloneSetup;

@RunWith(MockitoJUnitRunner.class)
public class ServerControllerComponentTest {

	public static final String URI_PUSHDATA = "http://localhost:8090/dataserver/pushdata";
	public static final String URI_GETDATA = "http://localhost:8090/dataserver/data/{blockType}";
	public static final String URI_PATCHDATA = "http://localhost:8090/dataserver/update/{name}/{newBlockType}";

	@Mock
	private Server serverMock;

	private DataEnvelope testDataEnvelope;
	private ObjectMapper objectMapper;
	private MockMvc mockMvc;
	private ServerController serverController;

	@Before
	public void setUp() throws HadoopClientException, NoSuchAlgorithmException, IOException {
		serverController = new ServerController(serverMock);
		mockMvc = standaloneSetup(serverController).build();
		objectMapper = Jackson2ObjectMapperBuilder
				.json()
				.build();

		testDataEnvelope = TestDataHelper.createTestDataEnvelopeApiObject();
	}

	@Test
	public void testPushDataPostCallWorksAsExpected() throws Exception {
		when(serverMock.saveDataEnvelope(any(DataEnvelope.class))).thenReturn(true);

		String testDataEnvelopeJson = objectMapper.writeValueAsString(testDataEnvelope);

		MvcResult mvcResult = mockMvc.perform(post(URI_PUSHDATA)
				.content(testDataEnvelopeJson)
				.contentType(MediaType.APPLICATION_JSON_VALUE))
				.andExpect(status().isOk())
				.andReturn();

		boolean checksumPass = Boolean.parseBoolean(mvcResult.getResponse().getContentAsString());
		assertThat(checksumPass).isTrue();
	}

	@Test
	public void testPushDataPostCallHadoopFail() throws Exception {
		when(serverMock.saveDataEnvelope(any(DataEnvelope.class))).thenThrow(HadoopClientException.class);

		String testDataEnvelopeJson = objectMapper.writeValueAsString(testDataEnvelope);

		MvcResult mvcResult = mockMvc.perform(post(URI_PUSHDATA)
						.content(testDataEnvelopeJson)
						.contentType(MediaType.APPLICATION_JSON_VALUE))
				.andExpect(status().isOk())
				.andReturn();

		boolean checksumPass = Boolean.parseBoolean(mvcResult.getResponse().getContentAsString());
		assertThat(checksumPass).isFalse();
	}

	@Test
	public void testGetData() throws Exception {
		List<DataEnvelope> testDataEnvelopeList = new ArrayList<>();
		testDataEnvelopeList.add(testDataEnvelope);
		when(serverMock.getDataByBlockType(any(String.class))).thenReturn(testDataEnvelopeList);

		mockMvc.perform(get(URI_GETDATA, "BLOCKTYPEA")
						.contentType(MediaType.APPLICATION_JSON))
						.andExpect(status().isOk())
						.andExpect(jsonPath("$.size()", Matchers.is(1)))
						.andReturn();
	}

	@Test
	public void testPatchDataUpdateSuccess() throws Exception {

		when(serverMock.updateDataByBlockName(any(String.class), any(String.class))).thenReturn(true);

		MvcResult mvcResult = mockMvc.perform(patch(URI_PATCHDATA, "TSLA-GBPUSD-10Y", "BLOCKTYPEB")
						.contentType(MediaType.APPLICATION_JSON))
						.andExpect(status().isOk())
						.andReturn();

		assertThat(Boolean.parseBoolean(mvcResult.getResponse().getContentAsString())).isTrue();
	}

	@Test
	public void testPatchDataUpdateFailNoRecord() throws Exception {

		when(serverMock.updateDataByBlockName(any(String.class), any(String.class))).thenReturn(false);

		MvcResult mvcResult = mockMvc.perform(patch(URI_PATCHDATA, "APPL-GBPUSD-10Y", "BLOCKTYPEB")
						.contentType(MediaType.APPLICATION_JSON))
				.andExpect(status().isOk())
				.andReturn();

		assertThat(Boolean.parseBoolean(mvcResult.getResponse().getContentAsString())).isFalse();
	}
}
