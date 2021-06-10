package com.ss.repository.secondary;

import com.ss.model.User;
import org.springframework.data.mongodb.repository.MongoRepository;

/**
 * @author neo
 */
public interface SecondaryRepository extends MongoRepository<User, String> {
}
