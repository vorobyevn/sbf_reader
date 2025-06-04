// Получение результата сравнения документов и сохранение результата
// Не загружаем 'pub_datetime' чтобы не обновлять поднятые обьявления
function save_modified(coll_old, coll_new) {
	var doc_old, doc_new, total_count = 0, m = 0;
	var find_fields = {'_id':0, 'my_addr_id':0, 'load':0, 'merge_stat':0, 'pub_datetime': 0};
	var cur_new = db[coll_new].find({"merge_stat": "s"}, find_fields).limit(10000);
	while (cur_new.hasNext()) {
		doc_new = cur_new.next();
		doc_old = db[coll_old].findOne({"w6_offer_id": doc_new["w6_offer_id"]}, find_fields);
		if (doc_old != null) {
			var diff = diff_changed(doc_old, doc_new);
			if (diff['changed'] == 'equal') {
				db[coll_new].update({"w6_offer_id": doc_new["w6_offer_id"]}, {'$set': {'merge_stat': 'e'}})
			}
			else {
				db[coll_new].update({"w6_offer_id": doc_new["w6_offer_id"]}, {'$set': {'merge_stat': 'm'}})
				m++;
			}
		}
		total_count++;
	}

	return { modify: m, total:total_count };
};