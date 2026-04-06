//macro that generates a select statement for a single row
macro_rules! sql_query_one {
    ($conn:expr, $table:expr, $fields:expr, where { $($where_field:ident $(= $where_value:expr)?),+ $(,)? }, $mapper:expr) => {{
        let wheres = vec![$(concat!(stringify!($where_field), " = :", stringify!($where_field))),+].join(" AND ");
        let sql = format!("SELECT {} FROM {} WHERE {}", $fields, $table, wheres);

        $conn.query_one(
            &sql,
            &[$(
                (concat!(":", stringify!($where_field)), &sql_query_one!(@value $where_field $(= $where_value)?) as &dyn rusqlite::ToSql)
            ),+] as &[(&str, &dyn rusqlite::ToSql)],
            $mapper
        )
    }};

    (@value $field:ident = $value:expr) => { $value };
    (@value $field:ident) => { $field };
}

macro_rules! sql_query {
    ($conn:expr, $table:expr, $fields:expr, where { $($where_field:ident $(= $where_value:expr)?),+ $(,)? }, order by $order:expr, $mapper:expr) => {{
        let wheres = vec![$(concat!(stringify!($where_field), " = :", stringify!($where_field))),+].join(" AND ");
        let sql = format!("SELECT {} FROM {} WHERE {} ORDER BY {}", $fields, $table, wheres, $order);

        $conn.prepare(&sql).and_then(|mut stmt| stmt.query_map(
            &[$(
                (concat!(":", stringify!($where_field)), &sql_query!(@value $where_field $(= $where_value)?) as &dyn rusqlite::ToSql)
            ),+] as &[(&str, &dyn rusqlite::ToSql)],
            $mapper
        ).and_then(|r| r.collect::<Result<Vec<_>, _>>()))
    }};

    ($conn:expr, $table:expr, $fields:expr, where { $($where_field:ident $(= $where_value:expr)?),+ $(,)? }, $mapper:expr) => {{
        let wheres = vec![$(concat!(stringify!($where_field), " = :", stringify!($where_field))),+].join(" AND ");
        let sql = format!("SELECT {} FROM {} WHERE {}", $fields, $table, wheres);

        $conn.prepare(&sql).and_then(|mut stmt| stmt.query_map(
            &[$(
                (concat!(":", stringify!($where_field)), &sql_query!(@value $where_field $(= $where_value)?) as &dyn rusqlite::ToSql)
            ),+] as &[(&str, &dyn rusqlite::ToSql)],
            $mapper
        ).and_then(|r| r.collect::<Result<Vec<_>, _>>()))
    }};

    (@value $field:ident = $value:expr) => { $value };
    (@value $field:ident) => { $field };
}
