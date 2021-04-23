package com.qovery.oss

import com.fasterxml.jackson.databind.PropertyNamingStrategy
import com.fasterxml.jackson.databind.SerializationFeature
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import io.ktor.application.*
import io.ktor.features.*
import io.ktor.http.*
import io.ktor.response.*
import io.ktor.request.*
import io.ktor.routing.*
import io.ktor.jackson.*
import org.jetbrains.exposed.sql.*
import org.jetbrains.exposed.sql.`java-time`.datetime
import org.jetbrains.exposed.sql.transactions.transaction
import java.math.BigInteger
import java.security.MessageDigest
import java.time.LocalDateTime
import java.time.ZoneId
import java.util.*


fun main(args: Array<String>): Unit = io.ktor.server.netty.EngineMain.main(args)

fun String.encodeToID(truncateLength: Int = 6): String {
    // hash String with MD5
    val hashBytes = MessageDigest.getInstance("MD5").digest(this.toByteArray(Charsets.UTF_8))
    val hashString = String.format("%032x", BigInteger(1, hashBytes))
    return hashString.take(truncateLength)
}

data class Request(val url: String) {
    fun toResponse(): Response = Response(url, url.encodeToID())
}

data class Stat(val clicksOverTime: MutableList<Date> = mutableListOf())

data class Response(val originalURL: String, val id: String, val stat: Stat = Stat()) {
    val shortURL: String = "${System.getenv("QOVERY_ROUTER_MAIN_URL")}$id"
}

object ResponseTable : Table("response") {
    val id = varchar("id", 32)
    val originalURL = varchar("original_url", 2048)
    override val primaryKey: PrimaryKey = PrimaryKey(id)
}

object ClickOverTimeTable : Table("click_over_time") {
    val id = integer("id").autoIncrement()
    val clickDate = datetime("click_date")
    val response = reference("response_id", onDelete = ReferenceOption.CASCADE, refColumn = ResponseTable.id)
    override val primaryKey: PrimaryKey = PrimaryKey(id)
}

fun initDatabase() {
    val config = HikariConfig().apply {
        jdbcUrl = "jdbc:${System.getenv("QOVERY_DATABASE_URLSHORTENER_CONNECTION_URI_WITHOUT_CREDENTIALS")}"
        username = System.getenv("QOVERY_DATABASE_URLSHORTENER_USERNAME")
        password = System.getenv("QOVERY_DATABASE_URLSHORTENER_PASSWORD")
        driverClassName = "org.postgresql.Driver"
    }

    Database.connect(HikariDataSource(config))

    transaction {
        // create tables if they do not exist
        SchemaUtils.createMissingTablesAndColumns(ResponseTable, ClickOverTimeTable)
    }
}

@kotlin.jvm.JvmOverloads
fun Application.module(testing: Boolean = false) {
    initDatabase()

    install(ContentNegotiation) {
        jackson {
            enable(SerializationFeature.INDENT_OUTPUT)
            propertyNamingStrategy = PropertyNamingStrategy.SNAKE_CASE
            disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
        }
    }

//    val responseById = mutableMapOf<String, Response>()

    fun getResponseById(id: String): Response? {
        return transaction {
            ResponseTable.select {ResponseTable.id eq id}
                .limit(1)
                .map {
                    Response(originalURL = it[ResponseTable.originalURL], id = it[ResponseTable.id])
                }
        }.firstOrNull()
    }

    fun persistResponse(response: Response) {
        transaction {
            ResponseTable.insert {
                it[originalURL] = response.originalURL
                it[id] = response.id
            }
        }
    }

    fun getShortURL(url: String, truncateLength: Int = 6): String {
        val id = url.encodeToID(truncateLength)

        val retrievedResponse = getResponseById(id)
        if (retrievedResponse != null && retrievedResponse.originalURL != url) {
            // collision spotted !
            return getShortURL(url, truncateLength + 1)
        }
        return id
    }

    routing {

        get("/{id}") {
            val id = call.parameters["id"]
            val retrievedResponse = id?.let { getResponseById(it) }

            if (id.isNullOrBlank() || retrievedResponse == null) {
                return@get call.respondRedirect("http://www.duckduckgo.com")
            }
            log.debug("retrievedResponse: $retrievedResponse")
            retrievedResponse.stat.clicksOverTime.add(Date())

            // add current date to the current response stats
            transaction {
                ClickOverTimeTable.insert {
                    it[clickDate] = LocalDateTime.now()
                    it[response] = retrievedResponse.id
                }
            }

            log.debug("redirect to: ${retrievedResponse.originalURL}")
            call.respondRedirect(retrievedResponse.originalURL)
        }

        get("/api/v1/url/{id}/stat") {
            val id = call.parameters["id"]
            val retrievedResponse = id?.let { getResponseById(it) }

            if (id.isNullOrBlank() || retrievedResponse == null) {
                return@get call.respond(HttpStatusCode.NoContent)
            }

            val dates: List<Date> = transaction {
                ClickOverTimeTable.select { ClickOverTimeTable.response eq id}
                // convert LocalDateTime to Date
                    .map { Date.from(it[ClickOverTimeTable.clickDate].atZone(ZoneId.systemDefault()).toInstant())}
            }

            retrievedResponse.stat.clicksOverTime.addAll(dates)

            call.respond(retrievedResponse.stat)
        }

        post("/api/v1/encode") {
            // Deserialize JSON body to Request object
            val request = call.receive<Request>()

            // find the Response object if it already exists
            val shortURL = getShortURL(request.url)
            val retrievedResponse = getResponseById(shortURL)
            if (retrievedResponse != null) {
                log.debug("cache hit $retrievedResponse")
                return@post call.respond(retrievedResponse)
            }

            val response = request.toResponse()
//            responseById[shortURL] = response
            persistResponse(response)
            log.debug("cache miss $response")

            // Serialize Response object to JSON body
            call.respond(response)

        }

        get("/") {
            call.respondText("Hello, world!")
        }
    }
}


