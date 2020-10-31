/*
 *  Friendly exporter for Neo4j - Copyright (C) 2020  Hugo JOBY
 *
 *      This library is free software; you can redistribute it and/or modify it under the terms
 *      of the GNU Lesser General Public License as published by the Free Software Foundation;
 *      either version 2.1 of the License, or (at your option) any later version.
 *      This library is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 *      without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 *      See the GNU Lesser General Public License for more details.
 *
 *      You should have received a copy of the GNU Lesser General Public License along with this library;
 *      If not, see <https://www.gnu.org/licenses/>.
 */

package com.castsoftware.exporter.exceptions.neo4j;

import com.castsoftware.exporter.exceptions.ExporterException;


/**
 * The <code>Neo4jConnectionError</code> is thrown when the connection between the procedure and the database fail.
 * Neo4jConnectionError
 */
public class Neo4jConnectionError extends ExporterException {

    private static final long serialVersionUID = 7522702117300762310L;
    private static final String messagePrefix = "Error, the connection with neo4j failed : ";
    private static final String codePrefix = "NEO_CE_";

    public Neo4jConnectionError(String message, Throwable cause, String code) {
        super(messagePrefix.concat(message), cause, codePrefix.concat(code));
    }

    public Neo4jConnectionError(String message, String code) {
        super(messagePrefix.concat(message), codePrefix.concat(code));
    }

}
