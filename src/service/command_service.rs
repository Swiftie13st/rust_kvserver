use crate::*;

impl CommandService for Hget {
    fn execute(self, store: &impl Storage) -> CommandResponse {
        match store.get(&self.table, &self.key) {
            Ok(Some(v)) => v.into(),
            Ok(None) => KvError::NotFound(self.table, self.key).into(),
            Err(e) => e.into(),
        }
    }
}

impl CommandService for Hgetall {
    fn execute(self, store: &impl Storage) -> CommandResponse {
        match store.get_all(&self.table) {
            Ok(v) => v.into(),
            Err(e) => e.into(),
        }
    }
}

impl CommandService for Hset {
    fn execute(self, store: &impl Storage) -> CommandResponse {
        match self.pair {
            Some(v) => match store.set(&self.table, v.key, v.value.unwrap_or_default()) {
                Ok(Some(v)) => v.into(),
                Ok(None) => Value::default().into(),
                Err(e) => e.into(),
            },
            None => Value::default().into(),
        }
    }
}

impl CommandService for Hmget {
    fn execute(self, store: &impl Storage) -> CommandResponse {
        let table = self.table;
        self.keys
            .into_iter()
            .map(|key| match store.get(&table, &key) {
                Ok(Some(v)) => v,
                Ok(None) => Value::default(),
                Err(_) => Value::default(),
            })
            .collect::<Vec<_>>()
            .into()
    }
}

impl CommandService for Hmset {
    fn execute(self, store: &impl Storage) -> CommandResponse {
        let pairs = self.pairs;
        let table = self.table;

        pairs
            .into_iter()
            .map(|pair| {
                let result = store.set(&table, pair.key, pair.value.unwrap_or_default());
                match result {
                    Ok(Some(v)) => v,
                    _ => Value::default(),
                }
            })
            .collect::<Vec<_>>()
            .into()
    }
}

impl CommandService for Hdel {
    fn execute(self, store: &impl Storage) -> CommandResponse {
        match store.del(&self.table, &self.key) {
            Ok(Some(v)) => v.into(),
            Ok(None) => KvError::NotFound(self.table, self.key).into(),
            Err(e) => e.into(),
        }
    }
}

impl CommandService for Hmdel {
    fn execute(self, store: &impl Storage) -> CommandResponse {
        let table = self.table;
        self.keys
            .into_iter()
            .map(|key| match store.del(&table, &key) {
                Ok(Some(v)) => v,
                Ok(None) => Value::default(),
                Err(_) => Value::default(),
            })
            .collect::<Vec<_>>()
            .into()
    }
}

impl CommandService for Hexist {
    fn execute(self, store: &impl Storage) -> CommandResponse {
        match store.contains(&self.table, &self.key) {
            Ok(exists) => Value::from(exists).into(),
            Err(e) => e.into(),
        }
    }
}

impl CommandService for Hmexist {
    fn execute(self, store: &impl Storage) -> CommandResponse {
        let table = self.table;
        let keys = self.keys;

        let exists = keys
            .into_iter()
            .map(|key| match store.contains(&table, &key) {
                Ok(e) => e.into(),
                Err(_) => Value::default(),
            })
            .collect::<Vec<Value>>();

        exists.into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hset_should_work() {
        let store = MemTable::new();
        let cmd = CommandRequest::new_hset("t1", "hello", "world".into());
        let res = dispatch(cmd.clone(), &store);
        assert_res_ok(res, &[Value::default()], &[]);

        let res = dispatch(cmd, &store);
        assert_res_ok(res, &["world".into()], &[]);
    }

    #[test]
    fn hget_should_work() {
        let store = MemTable::new();
        let cmd = CommandRequest::new_hset("score", "u1", 10.into());
        dispatch(cmd, &store);
        let cmd = CommandRequest::new_hget("score", "u1");
        let res = dispatch(cmd, &store);
        assert_res_ok(res, &[10.into()], &[]);
    }

    #[test]
    fn hget_with_non_exist_key_should_return_404() {
        let store = MemTable::new();
        let cmd = CommandRequest::new_hget("score", "u1");
        let res = dispatch(cmd, &store);
        assert_res_error(res, 404, "Not found");
    }

    #[test]
    fn hgetall_should_work() {
        let store = MemTable::new();
        let cmds = vec![
            CommandRequest::new_hset("score", "u1", 10.into()),
            CommandRequest::new_hset("score", "u2", 8.into()),
            CommandRequest::new_hset("score", "u3", 11.into()),
            CommandRequest::new_hset("score", "u1", 6.into()),
        ];
        for cmd in cmds {
            dispatch(cmd, &store);
        }

        let cmd = CommandRequest::new_hgetall("score");
        let res = dispatch(cmd, &store);
        let pairs = &[
            Kvpair::new("u1", 6.into()),
            Kvpair::new("u2", 8.into()),
            Kvpair::new("u3", 11.into()),
        ];
        assert_res_ok(res, &[], pairs);
    }

    #[test]
    fn hmset_should_work() {
        let store = MemTable::new();
        let pairs = &[
            Kvpair::new("u1", 6.into()),
            Kvpair::new("u2", 8.into()),
            Kvpair::new("u3", 11.into()),
        ];
        let cmd = CommandRequest::new_hmset("score", pairs.into());

        let res = dispatch(cmd, &store);

        assert_res_ok(
            res,
            &[Value::default(), Value::default(), Value::default()],
            &[],
        );

        let cmd = CommandRequest::new_hgetall("score");
        let res = dispatch(cmd, &store);
        print!("{:?}", res)
    }

    #[test]
    fn hmget_should_work() {
        let store = MemTable::new();
        let pairs = &[
            Kvpair::new("u1", 6.into()),
            Kvpair::new("u2", 8.into()),
            Kvpair::new("u3", 11.into()),
        ];
        dispatch(CommandRequest::new_hmset("score", pairs.into()), &store);

        // println!("{:?}", dispatch(CommandRequest::new_hgetall("score"), &store) );

        let cmd = CommandRequest::new_hmget("score", vec!["u1".into(), "u2".into(), "u3".into()]);
        let res = dispatch(cmd, &store);
        assert_res_ok(res, &[6.into(), 8.into(), 11.into()], &[]);
    }

    #[test]
    fn hdel_should_work() {
        let store = MemTable::new();
        let cmd = CommandRequest::new_hset("score", "u1", 10.into());
        dispatch(cmd, &store);

        let cmd = CommandRequest::new_hdel("score", "u1");
        let res = dispatch(cmd, &store);
        println!("11: {:?}", res.clone());
        assert_res_ok(res, &[10.into()], &[]);

        let cmd = CommandRequest::new_hget("score", "u1");
        let res = dispatch(cmd, &store);
        println!("22: {:?}", res.clone());
        assert_res_error(res, 404, "Not found");
    }

    #[test]
    fn hmdel_should_work() {
        let store = MemTable::new();
        let cmds = vec![
            CommandRequest::new_hset("score", "u1", 10.into()),
            CommandRequest::new_hset("score", "u2", 8.into()),
            CommandRequest::new_hset("score", "u3", 11.into()),
        ];
        for cmd in cmds {
            dispatch(cmd, &store);
        }

        let cmd = CommandRequest::new_hmdel("score", vec!["u1".into(), "u2".into()]);
        let res = dispatch(cmd, &store);
        assert_res_ok(res, &[10.into(), 8.into()], &[]);

        let cmd = CommandRequest::new_hget("score", "u1");
        let res = dispatch(cmd, &store);
        assert_res_error(res, 404, "Not found");

        let cmd = CommandRequest::new_hget("score", "u2");
        let res = dispatch(cmd, &store);
        assert_res_error(res, 404, "Not found");

        let cmd = CommandRequest::new_hget("score", "u3");
        let res = dispatch(cmd, &store);
        assert_res_ok(res, &[11.into()], &[]);
    }

    #[test]
    fn hexist_should_work() {
        let store = MemTable::new();
        let cmd = CommandRequest::new_hset("score", "u1", 10.into());
        dispatch(cmd, &store);

        let cmd = CommandRequest::new_hexist("score", "u1");
        let res = dispatch(cmd, &store);
        assert_res_ok(res, &[true.into()], &[]);

        let cmd = CommandRequest::new_hexist("score", "u2");
        let res = dispatch(cmd, &store);
        assert_res_ok(res, &[false.into()], &[]);
    }

    #[test]
    fn hmexist_should_work() {
        let store = MemTable::new();
        let cmds = vec![
            CommandRequest::new_hset("score", "u1", 10.into()),
            CommandRequest::new_hset("score", "u2", 8.into()),
        ];
        for cmd in cmds {
            dispatch(cmd, &store);
        }

        let cmd = CommandRequest::new_hmexist("score", vec!["u1".into(), "u2".into(), "u3".into()]);
        let res = dispatch(cmd, &store);
        assert_res_ok(res, &[true.into(), true.into(), false.into()], &[]);
    }
}
