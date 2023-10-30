package com.db.dataplatform.techtest.server.component.impl;

import com.db.dataplatform.techtest.server.api.model.DataBody;
import com.db.dataplatform.techtest.server.api.model.DataEnvelope;
import com.db.dataplatform.techtest.server.api.model.DataHeader;
import com.db.dataplatform.techtest.server.exception.HadoopClientException;
import com.db.dataplatform.techtest.server.persistence.BlockTypeEnum;
import com.db.dataplatform.techtest.server.persistence.model.DataBodyEntity;
import com.db.dataplatform.techtest.server.persistence.model.DataHeaderEntity;
import com.db.dataplatform.techtest.server.service.DataBodyService;
import com.db.dataplatform.techtest.server.component.Server;
import com.db.dataplatform.techtest.server.service.DataHeaderService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.modelmapper.ModelMapper;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.client.RestTemplate;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@Slf4j
@Service
@RequiredArgsConstructor
public class ServerImpl implements Server {

    private final DataBodyService dataBodyServiceImpl;
    private final ModelMapper modelMapper;
    private static final String URL_HADOOPSERVER = "http://localhost:8090/hadoopserver/pushbigdata";
    private static final int MAX_TRIES = 3;

    /**
     * @param envelope
     * @return true if there is a match with the client provided checksum.
     */
    @Override
    public boolean saveDataEnvelope(DataEnvelope envelope) throws HadoopClientException {

        int count = 0;
        RestTemplate restTemplate = new RestTemplate();

        while (count < MAX_TRIES) {
            try {
                ResponseEntity<HttpStatus> response = restTemplate.postForEntity(URL_HADOOPSERVER, "Dummy HADOOP payload", HttpStatus.class);
                log.info("Data saved to HADOOP server. Response code: {}", response.getStatusCode());
                break;
            } catch (Exception exception) {
                count++;
                log.info("Attempt {} failed. HADOOP server exception: {}", count, exception.getMessage());
            }
        }
        if (count >= MAX_TRIES) {
            throw new HadoopClientException("Saving to the HADOOP server failed after multiple attempts. Cancelling the DataEnvelope save to the local Database");
        }

        // Save to persistence.
        persist(envelope);
        return true;
    }

    @Override
    public List<DataEnvelope> getDataByBlockType(String blockType) {
        List<DataEnvelope> dataEnvelopeList = new ArrayList<>();
        List<DataBodyEntity> dataBodyEntityList = dataBodyServiceImpl.getDataByBlockType(blockType);
        log.info("Number of entities found: {}", dataBodyEntityList.size());

        for (DataBodyEntity dataBodyEntity: dataBodyEntityList) {
            DataHeaderEntity dataHeaderEntity = dataBodyEntity.getDataHeaderEntity();
            DataHeader dataHeader = modelMapper.map(dataHeaderEntity, DataHeader.class);
            DataBody dataBody = modelMapper.map(dataBodyEntity, DataBody.class);

            DataEnvelope dataEnvelope = new DataEnvelope(dataHeader, dataBody);
            dataEnvelopeList.add(dataEnvelope);
        }

        return dataEnvelopeList;
    }

    @Override
    public boolean updateDataByBlockName(String name, String blockType) {
        Optional<DataBodyEntity> dataBodyEntityOptional = dataBodyServiceImpl.getDataByBlockName(name);

        if (dataBodyEntityOptional.isPresent()) {
            dataBodyEntityOptional.get().getDataHeaderEntity().setBlocktype(BlockTypeEnum.valueOf(blockType));
            saveData(dataBodyEntityOptional.get());
            log.info("BlockType successfully updated to: {}", blockType);
            return true;
        } else {
            log.info("No DataBody found with name: {}", name);
            return false;
        }
    }

    private void persist(DataEnvelope envelope) {
        log.info("Persisting data with attribute name: {}", envelope.getDataHeader().getName());
        DataHeaderEntity dataHeaderEntity = modelMapper.map(envelope.getDataHeader(), DataHeaderEntity.class);

        DataBodyEntity dataBodyEntity = modelMapper.map(envelope.getDataBody(), DataBodyEntity.class);
        dataBodyEntity.setDataHeaderEntity(dataHeaderEntity);

        saveData(dataBodyEntity);
    }

    private void saveData(DataBodyEntity dataBodyEntity) {
        dataBodyServiceImpl.saveDataBody(dataBodyEntity);
    }

}
