package example;

import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;
import org.neo4j.procedure.Description;

import java.util.List;

public class sameItemUrLookingAt {

    @Context
    public Transaction tx;

    public record EntityContainer(String title, List<String> genres, List<String> actors, List<String> directors,  Long commonGenres, Long commonActors, Long commonDirectors) {
    }

    @Procedure(name = "recommend.sameItemUrLookingAt", mode = Mode.READ)
    @Description("Returns similar movies based on common genres.")
    public Stream<EntityContainer> sameItemUrLookingAt(@Name("movieTitle") String movieTitle) {
        String query = """
                MATCH (m:Movie {title: $movieTitle})-[:IN_GENRE]->(g:Genre)
                MATCH (m)-[:IN_GENRE]->(g:Genre)<-[:IN_GENRE]-(rec:Movie)
                OPTIONAL MATCH (m)<-[:ACTED_IN]-(a:Actor)-[:ACTED_IN]->(rec)
                OPTIONAL MATCH (m)<-[:DIRECTED]-(d:Director)-[:DIRECTED]->(rec)
                WITH rec,
                collect(DISTINCT g.name) AS genres,
                collect(DISTINCT a.name) AS actors,
                collect(DISTINCT d.name) AS directors,
                size(collect(DISTINCT g)) AS commonGenres,
                size(collect(DISTINCT a)) AS commonActors,
                size(collect(DISTINCT d)) AS commonDirectors
                WHERE rec.title IS NOT NULL
                RETURN rec.title, genres, actors, directors, commonGenres, commonActors, commonDirectors
                ORDER BY commonGenres DESC, commonActors DESC, commonDirectors DESC
                LIMIT 10
                """;
        Result result = tx.execute(query, Map.of("movieTitle", movieTitle));

        return result.stream().map(row -> {
            String title = (String) row.get("rec.title");
            List<String> genres = (List<String>) row.get("genres");
            List<String> actors = (List<String>) row.get("actors");
            List<String> directors = (List<String>) row.get("directors");
            Long commonGenres = (Long) row.get("commonGenres");
            Long commonActors = (Long) row.get("commonActors");
            Long commonDirectors = (Long) row.get("commonDirectors");

            return new EntityContainer(title, genres, actors, directors, commonGenres, commonActors, commonDirectors);
        });
    }
}
