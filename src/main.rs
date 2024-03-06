use bb8::PooledConnection;
use diesel::{
    dsl::{date, now},
    result::Error,
    update, ExpressionMethods, NullableExpressionMethods, OptionalExtension, QueryDsl,
};
use diesel_async::{
    pooled_connection::{bb8::Pool, AsyncDieselConnectionManager},
    scoped_futures::ScopedFutureExt,
    AsyncConnection, AsyncPgConnection, RunQueryDsl,
};

diesel::table! {
    codigos (id) {
        id -> Int4,
        created_at_ts -> Timestamp,
        #[max_length = 26]
        code -> Varchar,
        expires_at -> Date,
        expired -> Bool,
    }
}

fn main() -> anyhow::Result<()> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    rt.block_on(async_main())
}

async fn async_main() -> anyhow::Result<()> {
    let pg_user = String::new();
    let pg_password = String::new();
    let pg_ip = String::new();
    let pg_port = String::new();
    let pg_db = String::new();

    let db_url = format!(
        "postgresql://{}:{}@{}:{}/{}",
        pg_user, pg_password, pg_ip, pg_port, pg_db
    );
    let config = AsyncDieselConnectionManager::<AsyncPgConnection>::new(db_url);
    let pool = Pool::builder().build(config).await?;
    let mut conn = pool.get().await?;
    let result: i32 = expire_one_code(&mut conn).await?;
    Ok(())
}

pub async fn expire_one_code(
    conn: &mut PooledConnection<'_, AsyncDieselConnectionManager<AsyncPgConnection>>,
) -> Result<i32, Error> {
    conn.transaction(move |mut conn| {
        async move {
            let unique_id = codigos::dsl::codigos
                .filter(codigos::expired.eq(false))
                .filter(codigos::expires_at.lt(date(now)))
                .select(codigos::id)
                .for_no_key_update()
                .skip_locked()
                .single_value();

            // println!("{}", debug_query::<Pg, _>(&unique_id).to_string());

            let updated = update(codigos::dsl::codigos)
                .filter(codigos::id.nullable().eq(unique_id))
                .set(codigos::expired.eq(true))
                .returning(codigos::id)
                .get_result(&mut conn)
                .await
                .optional()?;

            // let id_query = debug_query::<Pg, _>(&updated).to_string();
            // warn!("Id query: {}", id_query);

            let result = updated.unwrap_or_else(|| i32::default());
            Ok(result)
        }
        .scope_boxed()
    })
    .await
}
