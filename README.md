# Friendly Exporter for Neo4j (*[@tcalmant's fork](https://github.com/tcalmant/Friendly-Neo4j-Exporter)*)
### Neo4j extension offering a better CSV export than the CSV exporter integrated in Neo4J

![Folder structure with exporter](folder_structure.png)

A Neo4j extension to offer a better CSV export, with one file per label relationship. 
It provides an easier and more understandable way to edit the output with a CSV editor.

## Installation

To install the Friendly exporter, you have two options:

* Build the Java project with [Maven](https://maven.apache.org/).
Neo4j needs Java 8 to work properly, so make sure you have the correct JDK version. 
Once the build is complete, you should see a `.jar` package named `friendly-neo4j-exporter-%VERSION%.jar` 
in the `target` folder.
Drag & drop this file in the Neo4j plugin folder
(by default this folder is located in `%NEO4J_INSTALLATION_FOLDER%/neo4j/plugins`, or in `/plugins`
in Docker containers)
* Download the latest packaged extension [here](https://github.com/tcalmant/Friendly-Neo4j-Exporter/releases), 
 and drop this file in your Neo4j plugin folder.

After this operation, you'll have to **restart** your Neo4J instance.
If you try to replace an existing plugin file, you'll have to stop the instance first.

**Warning:** Don't forget to allow this extension in your Neo4j configuration file.
For more information, please refer to the
[official documentation](https://neo4j.com/docs/operations-manual/4.1/security/securing-extensions/)

## Usage

### Export

The complete procedure signature and options :
```python
# Save labels to CSV file format
CALL fexporter.save(LabelsToSave, Path, ZipFileName, SaveRelationship, ConsiderNeighbors, TrimValues) 

# Example of use : 
CALL fexporter.save(["C_relationship", "F_FrameworkRule"], "C:/Neo4j_exports/", "Result_05_09_20", true, true )
```

#### Parameters

* **@LabelsToSave** - *String List* 
  * Labels to save, as a list of string. 
  * Example: `["C_relationship", "F_FrameworkRule"]`
* **@Path** - *String*
  * Location to save output results.
  * Example: `/tmp/export`
* **@ZipFileName** - *String Optional*
  * Name of the final zip file (the extension `.zip` will be automatically added). 
  * If no value is given (empty string), then the CSV files will be kept as is in the output folder.
  * Example: `"Result_05_09_20"`, `""`
* **@SaveRelationship** - *Boolean Optional*
  * Save relationships associated to the labels selected.
  If the option **@ConsiderNeighbors** is active, relationships involving neighbors' label will also be 
  saved in the process
* **@ConsiderNeighbors** - *Boolean Optional*
  * Consider the neighbors of selected labels. 
  If a node in the provided label list has a relationship with another node from a different label,
  this label will also be saved.
  * This option does not necessitate the activation of **@SaveRelationship** to work, but it is
  strongly recommended in order to keep the report consistent.

You can now explore the content of the zip, extract it, and zip it back for re-import.
Each node/relationship saved will have its own associated file.

**Warning:** **Do not** modify the values inside the ID column in the node file nor the 
Source/Destination columns in the relationships file.
Those IDs are used to rebuild the graph later. If you modify them, you'll lose the consistency of the graph. 
*The IDs displayed are temporary, they will be regenerated for each node during the insertion into the database*.

### Import

```python
# Import back into Neo4K
CALL fexporter.load("C:/exports/output.zip")

# Example of use : 
CALL fexporter.load("C:/Neo4j_exports/config.zip")
```

#### Parameters

* **@PathToZipFileName** - *String*
  * Location to saved output results (in zip format).
  * Example: "C:/Neo4j_exports/Result_05_09_20.zip"

This way your data will be re-import back into Neo4j.

## TODO
 - Offer the possibility to select manually which relationship should be exported.
 - Save other nodes labels
 - Implement tests (maybe)

## Contributing

Pull requests are welcome.
For major changes, please open an issue first to discuss what you would like to change.
Don't hesitate to propose new functionalities, it's pretty basic for the moment,
but it would be great to have a more 'human friendly' exporter than the current integrated to Neo4J. 

## License

[GNU GPL](https://www.gnu.org/licenses/gpl-3.0.html)
