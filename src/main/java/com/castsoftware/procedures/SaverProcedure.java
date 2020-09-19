package com.castsoftware.procedures;

import com.castsoftware.exceptions.ProcedureException;
import com.castsoftware.exporter.Exporter;
import com.castsoftware.results.OutputMessage;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.List;
import java.util.stream.Stream;

public class SaverProcedure {

    @Context
    public GraphDatabaseService db;

    @Context
    public Log log;

    /**
     * Neo4 Procedure entry point for "fexporter.save()". See Neo4j documentation for more information.
     * @throws ProcedureException

     @Description("fexporter.save(LabelsToSave, Path, ZipFileName, SaveRelationship, ConsiderNeighbors) - Save labels to CSV file format. \n" +
     "Parameters : \n" +
     "               - @LabelsToSave- <String List> - Labels to save, as a list of string. Ex : [\"C_relationship\", \"F_FrameworkRule\"] " +
     "               - @Path - <String> - Location to save output results. Ex : \"C:\\User\\John\"" +
     "               - @ZipFileName - <String> - Name of the final zip file (the extension .zip will be automatically added). Ex : \"Result_05_09\" " +
     "               - @SaveRelationship - <Boolean> - Save relationships associated to the labels selected. If the option @ConsiderNeighbors is active, relationships involving neighbors' label will also be saved in the process" +
     "               - @ConsiderNeighbors - <Boolean> - Consider the neighbors of selected labels. If a node in the provided label list has a relationship with another node from a different label, this label will also be saved. " +
     "                                                  This option does not necessitate the activation of @SaveRelationship to work, but it is strongly recommended to keep the report consistent." +
     "Example of use : CALL fexporter.save([\"C_relationship\", \"F_FrameworkRule\"], \"C:/Neo4j_exports/\", \"MyReport\", true, true )" +
     "")**/
     @Procedure(value = "fexporter.save", mode = Mode.WRITE)
     public Stream<OutputMessage> saveProcedure(@Name(value = "LabelsToSave") List<String> labelList,
                                                @Name(value = "Path") String path,
                                                @Name(value = "ZipFileName",defaultValue="export") String zipFileName,
                                                @Name(value = "SaveRelationship", defaultValue="true") Boolean saveRelationShip,
                                                @Name(value = "ConsiderNeighbors", defaultValue="false") Boolean considerNeighbors) throws ProcedureException{
         Exporter exporter = new Exporter(db, log);
         return exporter.save(labelList, path, zipFileName, saveRelationShip, considerNeighbors);
     }

     public SaverProcedure() { } // Neo4J POJO **/
}
