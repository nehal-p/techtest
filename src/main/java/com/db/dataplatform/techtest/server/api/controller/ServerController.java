package com.db.dataplatform.techtest.server.api.controller;

import com.db.dataplatform.techtest.server.api.model.DataEnvelope;
import com.db.dataplatform.techtest.server.component.Server;
import com.db.dataplatform.techtest.server.exception.HadoopClientException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;
import javax.validation.Valid;
import javax.validation.constraints.Pattern;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.List;

@Slf4j
@Controller
@RequestMapping("/dataserver")
@RequiredArgsConstructor
@Validated
public class ServerController {

    private final Server server;

    @PostMapping(value = "/pushdata", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<Boolean> pushData(@Valid @RequestBody DataEnvelope dataEnvelope) throws IOException, NoSuchAlgorithmException {

        log.info("Data envelope received: {}", dataEnvelope.getDataHeader().getName());
        boolean success;
        try {
            success = server.saveDataEnvelope(dataEnvelope);
            log.info("Data envelope persisted. Attribute name: {}", dataEnvelope.getDataHeader().getName());
        } catch (HadoopClientException e) {
            success = false;
            log.info("Please try again later. Data push failed with exception: {}", e.getMessage());
        }

        return ResponseEntity.ok(success);
    }

    @GetMapping(value = "/data/{blockType}", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<List<DataEnvelope>> getData(@PathVariable("blockType") String blockType) {

        log.info("Data block type received: {}", blockType);
        List<DataEnvelope> dataEnvelopeList = server.getDataByBlockType(blockType);

        log.info("Data envelope list size: {}", dataEnvelopeList.size());
        return ResponseEntity.ok(dataEnvelopeList);
    }

    @PatchMapping(value = "/update/{name}/{newBlockType}", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<Boolean> updateData(@PathVariable("name") @Pattern(regexp = "^[A-Z0-9]{2,4}-[A-Z]{6}-[A-Z0-9]{3}$") String name, @PathVariable("newBlockType") @Pattern(regexp = "BLOCKTYPEA|BLOCKTYPEB") String newBlockType) throws IOException {

        log.info("Data body name : {}, New block type: {}", name, newBlockType);
        boolean success = server.updateDataByBlockName(name, newBlockType);

        return ResponseEntity.ok(success);
    }
}
