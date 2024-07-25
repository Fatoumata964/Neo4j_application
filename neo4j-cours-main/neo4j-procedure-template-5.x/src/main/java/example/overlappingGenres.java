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

public class overlappingGenres {

    @Context
    public Transaction tx;

    public record EntityContainer(String recommendation, List<String> scoreGenre, Long genreCount) {
    }

    @Procedure(name = "recommend.overlappingGenres", mode = Mode.READ)
    @Description("Returns similar movies based on common genres.")
    public Stream<EntityContainer> overlappingGenres(@Name("userId") String userId) {
        String query = """
                MATCH (u:User {userId: $userId})-[:RATED]->(m:Movie)-[:IN_GENRE]->(g:Genre)<-[:IN_GENRE]-(similar:Movie)
                WHERE NOT (u)-[:RATED]->(similar)
                WITH similar, COLLECT(DISTINCT g.name) AS genres
                RETURN similar.title AS recommendedMovie, genres, SIZE(genres) AS genreCount
                ORDER BY genreCount DESC
                LIMIT 10;
                """;
        Result result = tx.execute(query, Map.of("userId", userId));

        return result.stream().map(row -> {
            String recommendation = (String) row.get("recommendedMovie");
            List<String> scoreGenre = (List<String>) row.get("genres");
            Long genreCount = (Long) row.get("genreCount");

            return new EntityContainer(recommendation, scoreGenre, genreCount);
        });
    }
}
