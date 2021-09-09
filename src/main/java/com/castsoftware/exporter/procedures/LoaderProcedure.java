package com.castsoftware.exporter.procedures;

import com.castsoftware.exporter.exceptions.ProcedureException;
import com.castsoftware.exporter.io.Importer;
import com.castsoftware.exporter.results.OutputMessage;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.util.stream.Stream;

/**
 * Handles the <code>fexporter.load</code> procedure
 */
public class LoaderProcedure {

    /**
     * Access to the Neo4J database
     */
    @Context
    public GraphDatabaseService db;

    /**
     * Access to the Neo4J logger
     */
    @Context
    public Log log;


    /**
     * Default constructor to let Neo4J create the object
     */
    public LoaderProcedure() {
    }

    /**
     * Neo4 Procedure entry point for "fexporter.load()". See Neo4j documentation for more information.
     *
     * @throws ProcedureException Error Running the command
     **/
    @Procedure(value = "fexporter.load", mode = Mode.WRITE)
    @Description(value = "fexporter.load(ZipOrFolder) - Import a Zip file or a folder of CSV files to Neo4J.\n" +
            "Parameters:\n" +
            "    - @ZipOrFolder - <String> - Location of the Zip file or the CSVs folder. Ex: \"/tmp/config.zip\"" +
            "Example of use: CALL fexporter.load(\"/tmp/config.zip\")")
    public Stream<OutputMessage> loadProcedure(@Name(value = "ZipOrFolder") String pathToZipOrFolder) throws ProcedureException {
        Importer importer = new Importer(db, log);
        return importer.load(pathToZipOrFolder);
    }
}
