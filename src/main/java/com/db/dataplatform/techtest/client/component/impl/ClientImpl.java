package com.db.dataplatform.techtest.client.component.impl;

import com.db.dataplatform.techtest.client.RestTemplateConfiguration;
import com.db.dataplatform.techtest.client.api.model.DataEnvelope;
import com.db.dataplatform.techtest.client.component.Client;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.*;
import org.springframework.http.converter.StringHttpMessageConverter;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriTemplate;
import java.util.List;

/**
 * Client code does not require any test coverage
 */

@Service
@Slf4j
@RequiredArgsConstructor
public class ClientImpl implements Client {

    public static final String URI_PUSHDATA = "http://localhost:8090/dataserver/pushdata";
    public static final String URI_GETDATA = "http://localhost:8090/dataserver/data/{blockType}";
    public static final String URI_PATCHDATA = "http://localhost:8090/dataserver/update/{name}/{newBlockType}";

    @Autowired
    private RestTemplateConfiguration restTemplateConfiguration;

    @Override
    public void pushData(DataEnvelope dataEnvelope) {
        log.info("Pushing data {} to {}", dataEnvelope.getDataHeader().getName(), URI_PUSHDATA);
        RestTemplate restTemplate = restTemplateConfiguration.createRestTemplate(new MappingJackson2HttpMessageConverter(), new StringHttpMessageConverter());

        ResponseEntity<Boolean> response = restTemplate.postForEntity(URI_PUSHDATA, dataEnvelope, Boolean.class);
        log.info("Response: {}, {}", response.getStatusCodeValue(), response.getBody());
    }

    @Override
    public List<DataEnvelope> getData(String blockType) {
        log.info("Query for data with header block type {}", blockType);
        RestTemplate restTemplate = restTemplateConfiguration.createRestTemplate(new MappingJackson2HttpMessageConverter(), new StringHttpMessageConverter());

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<List<DataEnvelope>> request = new HttpEntity<>(headers);
        ResponseEntity<List<DataEnvelope>> response = restTemplate.exchange(URI_GETDATA, HttpMethod.GET, request, new ParameterizedTypeReference<List<DataEnvelope>>() {}, blockType);

        log.info("Data List of size {} received. Response: {}", response.getBody().size(), response.getStatusCodeValue());
        return response.getBody();
    }

    @Override
    public Boolean updateData(String blockName, String newBlockType) {
        log.info("Updating blocktype to {} for block with name {}", newBlockType, blockName);
        RestTemplate restTemplate = restTemplateConfiguration.createRestTemplate(new MappingJackson2HttpMessageConverter(), new StringHttpMessageConverter());

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<List<DataEnvelope>> request = new HttpEntity<>(headers);

        try {
            ResponseEntity<Boolean> response = restTemplate.exchange(URI_PATCHDATA, HttpMethod.PATCH, request, new ParameterizedTypeReference<Boolean>() {}, blockName, newBlockType);
            log.info("Data Update Response Code : {}, SUCCESS: {}", response.getStatusCodeValue(), response.getBody());
            return response.getBody();
        } catch (Exception e) {
            log.info("Exception occurred while attempting to update data: {}", e.getMessage());
            return false;
        }
    }
}
