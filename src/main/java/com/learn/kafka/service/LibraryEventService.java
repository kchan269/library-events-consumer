package com.learn.kafka.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learn.kafka.entity.LibraryEvent;
import com.learn.kafka.jpa.LibraryEventRepository;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.stereotype.Service;

import java.util.Optional;

@Service
@Slf4j
public class LibraryEventService {

    @Autowired
    ObjectMapper objectMapper;

    @Autowired
    LibraryEventRepository libraryEventRepository;



    public void processLibraryEvent(ConsumerRecord<Integer, String> consumerRecord) throws JsonProcessingException {
        LibraryEvent libraryEvent = objectMapper.readValue(consumerRecord.value(), LibraryEvent.class);
        log.info("consumer Event :{}", libraryEvent);

        if(libraryEvent.getLibraryEventId()!=null && libraryEvent.getLibraryEventId()==000){
            throw new RecoverableDataAccessException("Temporary Network Issue");
        }
        switch(libraryEvent.getLibraryEventType()) {
            case NEW:
                // save operation
                save(libraryEvent);
                break;
            case UPDATE:
                //validate 
                validate(libraryEvent);
                save(libraryEvent);
                break;
            default:
                log.info("invalid library event type");
        }
    }

    private void validate(LibraryEvent libraryEvent) {
            if (libraryEvent.getLibraryEventId() == null) {
                throw new IllegalArgumentException("Library Event id is missing");
            }
            Optional<LibraryEvent> libraryEventOptional = libraryEventRepository.findById(libraryEvent.getLibraryEventId());
            if (!libraryEventOptional.isPresent()) {
                throw new IllegalArgumentException("Library Event id not valid");
            }
            log.info("Validation is successful for the library event : {}", libraryEventOptional.get());
    }

    private void save(LibraryEvent libraryEvent) {
       libraryEvent.getBook().setLibraryEvent(libraryEvent);
       libraryEventRepository.save(libraryEvent);
       log.info("successfully presisted the library Event {}", libraryEvent);
    }
}
