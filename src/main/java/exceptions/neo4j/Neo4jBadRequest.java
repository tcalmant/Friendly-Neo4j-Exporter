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

package exceptions.neo4j;

import exceptions.ExporterException;

/**
 * The <code>Neo4jBadRequest</code> is thrown when the Neo4j request fails.
 * The failed request can be found in logs.
 * Neo4jBadRequest
 */
public class Neo4jBadRequest extends ExporterException {
    private static final long serialVersionUID = -4369489315467963030L;
    private static final String messagePrefix = "Error during Neo4j request : ";
    private static final String codePrefix = "NEO_BR_";

    public Neo4jBadRequest(String request, Throwable cause, String code) {
        super(messagePrefix.concat(request), cause, codePrefix.concat(code));
    }

    public Neo4jBadRequest(String request, String query, Throwable cause, String code) {
        super(messagePrefix.concat(request).concat(" . Query : ").concat(query), cause, codePrefix.concat(code));
    }
}
