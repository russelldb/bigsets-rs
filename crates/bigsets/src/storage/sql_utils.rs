#[macro_export]
macro_rules! sqlf {
    // sqlf!("sql/file.sql", key = value, ...)
    ($path:literal $(, $key:ident = $val:expr )* $(,)?) => {
        format!(include_str!($path) $(, $key = $val )*)
    };
}

pub fn placeholders_1(n: usize) -> String {
    if n == 0 {
        "(?)".into()
    } else {
        std::iter::repeat("(?)")
            .take(n)
            .collect::<Vec<_>>()
            .join(", ")
    }
}

pub fn placeholders_2(n: usize) -> String {
    if n == 0 {
        "(?,?)".into()
    } else {
        std::iter::repeat("(?,?)")
            .take(n)
            .collect::<Vec<_>>()
            .join(", ")
    }
}
