service tunnel {
    map<binary, binary> task(1:string func, 2:map<binary, binary> kwargs),
    void incr_client_num(),
    void decr_client_num(),
    i64 get_client_num(),
}
