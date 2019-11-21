package com.example.demo;

import io.r2dbc.postgresql.PostgresqlConnectionConfiguration;
import io.r2dbc.postgresql.PostgresqlConnectionFactory;
import io.r2dbc.spi.*;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

@SpringBootApplication
public class PostgresExampleApplication {

    public static void main(String[] args) {
        SpringApplication.run(PostgresExampleApplication.class, args);
	//	PostgresTest.writesToDb();
		PostgresTest.blockWrite();

	}


    static class PostgresTest {

        static ConnectionFactory connectionFactory = new PostgresqlConnectionFactory(PostgresqlConnectionConfiguration.builder()
                .host("localhost")
                .port(5432)
                .username("postgres")
                .password("admin")
                .database("test")
                .build());


        static void blockWrite() {

            Mono<Connection> connectionMono = Mono.from(connectionFactory.create());

            //Doesn't create a table:
            connectionMono.flatMapMany(connection ->
                    connection.createStatement("CREATE TABLE person ( id SERIAL PRIMARY KEY, firstname VARCHAR(100) NOT NULL, lastname VARCHAR(100) NOT NULL);")
                            .execute())
                    .blockFirst();

            //Result:
            //Nothing is written to console (Process finished with exit code 0) and postgres db is unchanged
             connectionMono.flatMapMany(connection -> connection
					    .createStatement("INSERT INTO person (id, firstname, lastname) VALUES ($1, $2, $3)")
						.bind("$1", 1)
						.bind("$2", "Walter")
						.bind("$3", "White")
						.execute())
					.blockFirst();

            //Result:
            //Nothing is written to console (Process finished with exit code 0) and postgres db is unchanged

        }

        static void verifyCompleteWrite() {

            Mono<Connection> connectionMono = Mono.from(connectionFactory.create());

			//Result: creates the table as expected
            connectionMono.flatMapMany(connection ->
                    connection.createStatement("CREATE TABLE person ( id SERIAL PRIMARY KEY, firstname VARCHAR(100) NOT NULL, lastname VARCHAR(100) NOT NULL);")
                            .execute())
                    .as(StepVerifier::create)
                    .expectNextCount(1) //
                    .verifyComplete();

			//Result: Inserts as expected
			connectionMono.flatMapMany(connection -> connection
                    .createStatement("INSERT INTO person (id, firstname, lastname) VALUES ($1, $2, $3)")
                    .bind("$1", 1)
                    .bind("$2", "Walter")
                    .bind("$3", "White")
                    .execute())
                    .as(StepVerifier::create) //
                    .expectNextCount(1) //
                    .verifyComplete();

			//Result: Drops table
			connectionMono.flatMapMany(connection ->
					connection.createStatement("DROP TABLE IF EXISTS person;")
							.execute())
					.as(StepVerifier::create)
					.expectNextCount(1)
					.verifyComplete();
		}
    }

}

