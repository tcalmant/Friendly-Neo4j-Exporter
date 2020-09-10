/*
 *  Copyright (C) 2020  Hugo JOBY
 *
 *     This program is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU General Public License as published by
 *     the Free Software Foundation, either version 3 of the License, or
 *     (at your option) any later version.
 *
 *     This program is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU General Public License for more details.
 *
 *     You should have received a copy of the GNU General Public License
 *     along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package com.castsoftware.exceptions.neo4j;

import com.castsoftware.exceptions.ExporterException;


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
