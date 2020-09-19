package com.castsoftware.procedures;

import com.castsoftware.exceptions.ProcedureException;
import com.castsoftware.exporter.Importer;
import com.castsoftware.results.OutputMessage;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.stream.Stream;

public class LoaderProcedure {
    @Context
    public GraphDatabaseService db;

    @Context
    public Log log;

    /**
     * Neo4 Procedure entry point for "fexporter.load()". See Neo4j documentation for more information.
     * @throws ProcedureException

     @Description("fexporter.load(PathToZipFileName) - Import a configuration zip file to neo4j. \n" +
     "Parameters : \n" +
     "               - @PathToZipFileName - <String> - Location to saved output results. Ex : \"C:\\User\\John\\config.zip\"" +
     "Example of use : CALL fexporter.load(\"C:\\Neo4j_exports\\config.zip\")" +
     "") **/
     @Procedure(value = "fexporter.load", mode = Mode.WRITE)
     public Stream<OutputMessage> loadProcedure(@Name(value = "PathToZipFileName") String pathToZipFileName) throws ProcedureException {
         Importer importer = new Importer(db, log);
         return importer.load(pathToZipFileName);
     }

    /**
     * Neo4J Pojo
     */
    public LoaderProcedure() { }
}
