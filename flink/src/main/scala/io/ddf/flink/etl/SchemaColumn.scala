package io.ddf.flink.etl

import io.ddf.content.Schema.ColumnType

case class SchemaColumn(name: String,
                        cType: ColumnType,
                        oldIndex: Int,
                        currentIndex: Int,
                        default: String)
