package org.noandish.library.ndatabase

/**
 * Created by AliasgharMirzazade on 10/10/2018 AD.
 */
@Suppress("unused")
class Row(val name_row: String, val type_row: Int) {
    companion object {
        const val TYPE_STRING = 0
        const val TYPE_BOOLEAN = 1
        const val TYPE_INTEGER = 2
        const val TYPE_BYTE = 3
        const val TYPE_ARRAY_BYTE = 4
        const val TYPE_DOUBLE = 5
        const val TYPE_FLOAT = 6
        const val TYPE_LONG = 7
    }
}