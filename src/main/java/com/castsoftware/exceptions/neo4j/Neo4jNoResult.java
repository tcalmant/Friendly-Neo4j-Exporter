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

package com.castsoftware.exceptions.neo4j;

import com.castsoftware.exceptions.ExporterException;

/**
 * The <code>Neo4jNoResult</code> is thrown when a request doesn't return expected com.castsoftware.results or if com.castsoftware.results are empty.
 * Neo4jNoResult
 */
public class Neo4jNoResult extends ExporterException {

    private static final long serialVersionUID = 8218353918930322258L;
    private static final String messagePrefix = "Error, the query returned no com.castsoftware.results : ";
    private static final String codePrefix = "NEO_NR_";

    public Neo4jNoResult(String message, Throwable cause, String code) {
        super(messagePrefix.concat(message), cause, codePrefix.concat(code));
    }

    public Neo4jNoResult(String message, String query, String code) {
        super(messagePrefix.concat(message).concat(" . Query : ").concat(query), codePrefix.concat(code));
    }

    public Neo4jNoResult(String message, String query, Throwable cause, String code) {
        super(messagePrefix.concat(message).concat(" . Query : ").concat(query), cause, codePrefix.concat(code));
    }

}
