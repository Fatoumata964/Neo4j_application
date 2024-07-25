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

public class CommonGenre {

    @Context
    public Transaction tx;

    public static class EntityContainer {
        public String title;
        public List<String> genres;
        public Long Nbcommongenres;

        public EntityContainer(String title, List<String> genres, Long Nbcommongenres) {
            this.title = title;
            this.genres = genres;
            this.Nbcommongenres = Nbcommongenres;
        }
    }

    @Procedure(name = "recommend.simMoviesByComGenre", mode = Mode.READ)
    @Description("Returns similar movies based on common genres.")
    public Stream<EntityContainer> simMoviesByComGenre(@Name("movieTitle") String movieTitle) {
        String query = """
                    MATCH (m:Movie)-[:IN_GENRE]->(g:Genre) <-[:IN_GENRE]-(othermovie:Movie)
                    WHERE m.title = $movieTitle
                    WITH othermovie, collect(g.name) AS genres, count(*) AS Nbcommongenres 
                    RETURN othermovie.title AS title, genres, Nbcommongenres 
                    ORDER BY Nbcommongenres DESC LIMIT 10;
                    """;
        Result result = tx.execute(query, Map.of("movieTitle", movieTitle));

        return result.stream().map(row -> {
            String title = (String) row.get("title");
            List<String> genres = (List<String>) row.get("genres");
            Long Nbcommongenres = (Long) row.get("Nbcommongenres");

            return new EntityContainer(title, genres, Nbcommongenres);
        });
    }
}
