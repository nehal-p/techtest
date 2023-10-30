package com.db.dataplatform.techtest.server.component;

import com.db.dataplatform.techtest.server.api.model.DataEnvelope;
import com.db.dataplatform.techtest.server.exception.HadoopClientException;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.List;

public interface Server {
    boolean saveDataEnvelope(DataEnvelope envelope) throws IOException, NoSuchAlgorithmException, HadoopClientException;
    List<DataEnvelope> getDataByBlockType(String blockType);
    boolean updateDataByBlockName(String name, String blockType);
}
