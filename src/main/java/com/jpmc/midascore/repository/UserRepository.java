package com.jpmc.midascore.repository;

import com.jpmc.midascore.entity.UserRecord;
import org.springframework.data.repository.CrudRepository;



public interface UserRepository extends CrudRepository<UserRecord, Long> {
    UserRecord findById(long id);
}



//Repository for the UserRecord entity
//This is used to store and retrieve UserRecord objects from the database
// It maps UserRecord to the user table in the database (long is type of primary key)
//It extends the CrudRepository interface, which provides basic CRUD operations for the UserRecord entity
//The UserRecord entity is a JPA entity that represents a user in the database