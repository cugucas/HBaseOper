package web;

import domain.HBaseQueryBean;

/**
 * Created by LJian on 2016/5/18.

@RestController
@RequestMapping("/hbase")
public class HBaseController {

    @RequestMapping(value = "/{actorId}", method = RequestMethod.GET)
    public getDataByRowKey(@RequestParam("TableName") String tableName,
        @RequestParam("RowKey") String rowKey){
        HBaseQueryBean queryBean;


    }
}



public class ActorController {
    @Autowired
    private ActorMapper actorMapper;

    @RequestMapping(value = "/{actorId}", method = RequestMethod.GET)
    public Actor getAddressById(@PathVariable Integer actorId) {
        return actorMapper.findOne(actorId);
    }
}
*/
