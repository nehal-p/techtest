package com.db.dataplatform.techtest.server.persistence.repository;

import com.db.dataplatform.techtest.server.persistence.model.DataBodyEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import java.util.List;
import java.util.Optional;

@Repository
public interface DataStoreRepository extends JpaRepository<DataBodyEntity, Long> {

    @Query(value = "SELECT * FROM DATA_STORE s WHERE s.DATA_HEADER_ID IN (SELECT DATA_HEADER_ID FROM DATA_HEADER WHERE BLOCKTYPE = :blockType)", nativeQuery = true)
    List<DataBodyEntity> getDataByBlockType(@Param("blockType") String blockType);

    @Query(value = "SELECT * FROM DATA_STORE s WHERE s.DATA_HEADER_ID IN (SELECT DATA_HEADER_ID FROM DATA_HEADER WHERE NAME = :name)", nativeQuery = true)
    Optional<DataBodyEntity> getDataByBlockName(@Param("name") String name);
}
