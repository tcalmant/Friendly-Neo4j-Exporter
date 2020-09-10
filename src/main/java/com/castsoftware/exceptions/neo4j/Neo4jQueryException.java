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

public class Neo4jQueryException extends ExporterException {

    private static final long serialVersionUID = 8087192855448474860L;
    private static final String messagePrefix = "Error during Neo4j query : ";
    private static final String codePrefix = "NEO_BR_";

    public Neo4jQueryException(String request, Throwable cause, String code) {
        super(messagePrefix.concat(request), cause, codePrefix.concat(code));
    }

    public Neo4jQueryException(String request, String query, Throwable cause, String code) {
        super(messagePrefix.concat(request).concat(" . Query : ").concat(query), cause, codePrefix.concat(code));
    }
}
