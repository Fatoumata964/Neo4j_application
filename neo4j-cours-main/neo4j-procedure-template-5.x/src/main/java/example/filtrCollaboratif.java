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

public class filtrCollaboratif {

    @Context
    public Transaction tx;

    public record EntityContainer(String recommendation, Long NbUsersWhoWatched) {
    }


    @Procedure(name = "recommend.collaborativeFiltering", mode = Mode.READ)
    @Description("Returns similar movies based on common genres.")
    public Stream<EntityContainer> collaborativeFiltering(@Name("movieTitle") String movieTitle) {
        String query = """
                    MATCH (m:Movie {title: $movieTitle})<-[:RATED]- (u:User)-[:RATED]->(othermovie:Movie) 
                    WITH othermovie, COUNT(*) AS NbUsersWhoWatched 
                    ORDER BY NbUsersWhoWatched DESC 
                    LIMIT 10 
                    RETURN othermovie.title AS recommendation, NbUsersWhoWatched
                    """;
        Result result = tx.execute(query, Map.of("movieTitle", movieTitle));

        return result.stream().map(row -> {
            String recommendation = (String) row.get("recommendation");
            Long NbUsersWhoWatched = (Long) row.get("NbUsersWhoWatched");

            return new EntityContainer(recommendation, NbUsersWhoWatched);
        });
    }
}
