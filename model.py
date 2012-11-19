import datetime
from xmldict import dict_to_xml, xml_to_dict

from dal import DAL

db = DAL('sqlite://movie.db')

# TODO: Some field validation is needed
db.define_table('tbl_clip', \
                    db.Field('name', 'string', length = 32, unique = True), \
                    db.Field('description', 'string', length = 256), \
                    db.Field('start_time', 'datetime'), \
                    db.Field('stop_time', 'datetime')
                )
db.define_table('tbl_show', \
                    db.Field('name', 'string', length = 32, unique = True)
                )
db.define_table('tbl_producer', \
                    db.Field('name', 'string', length = 32, unique = True), \
                    db.Field('phone', 'string', length = 32), \
                    db.Field('email', 'string', length = 32)
                )
db.define_table('tbl_producer_show', \
                    db.Field('producer_id', 'reference tbl_producer'), \
                    db.Field('show_id', 'reference tbl_show')
                    )
db.define_table('tbl_selected_clip', \
                    db.Field('clip_id', 'reference tbl_clip'), \
                    db.Field('ps_id', 'reference tbl_producer_show')
                    )


class DBObject(object):
    
    _table = None
    
    def __init__(self):
        self._id = None

    def getDict(self):
        return { k : getattr(self, k) for k in self.getFields() if k != 'id' } 
    
    def __repr__(self):
        return "%s_%s<%s>" % (self.__class__.__name__, self._id, \
                     ", ".join([str(x) for x in self.getDict().values()]) )
        
    def getFields(self):
        return db[self._table]['fields']
        
    def save(self):
        fields = self.getDict()
        if self._id:
            db[self._table][self._id] = fields
        else:
            rid = db[self._table].insert(**fields)
            self._id = int(rid)
            
    def commit(self):
        db.commit()
            
    def sync(self):
        if not self._id and not self.name:
            raise Exception("No valid id")
        self.row = db[self._table][self._id]
        if self.row:
            fields = self.getFields()
            for field in fields:
                if field != 'id':
                    setattr(self, field, getattr(self.row, field))
                else:
                    self._id = self.row.id
        return self
    
    def get_xml(self):
        result = {'id' : self._id}
        result.update(self.getDict())   
        return dict_to_xml(result)
    
    def set_xml(self, input):
        result = xml_to_dict(input)
        for k, v in result.iteritems():
            if k != 'id' and k in self.getFields():
                setattr(self, k, v)
            elif k == 'id':
                self._id = v
        
    
class Clip(DBObject):
    
    _table = 'tbl_clip'
    
    def __init__(self, clip_id = None, name = None, \
                 description = None, start_time = None, \
                 stop_time = None):
        self._id = clip_id
        self.name = name
        self.description = description
        self.start_time = start_time
        self.stop_time = stop_time
        
    @property
    def clip_id(self):
        return self._id
    
    @clip_id.setter
    def clip_id(self, value):
        self._id = value

    @property
    def name(self):
        return self._name
    
    @name.setter
    def name(self, value):
        self._name = value
        
    @property
    def description(self):
        return self._description
    
    @description.setter
    def description(self, value):
        self._description = value

    @property
    def duration(self):
        if self.start_time and self.stop_time:
            duration = self.stop_time - self.start_time
            return duration
        else:
            return datetime.timedelta(seconds=0)
        

class Producer(DBObject):
    
    _table = 'tbl_producer'
    
    def __init__(self, producer_id = None, name = None, \
                 phone = None, email = None ):
        self._id = producer_id
        self.name = name
        self.phone = phone
        self.email = email
        
    @property
    def producer_id(self):
        return self._id
    
    @producer_id.setter
    def producer_id(self, value):
        self._id = value

    @property
    def name(self):
        return self._name
    
    @name.setter
    def name(self, value):
        self._name = value
        

class Show(DBObject):
    
    _table = 'tbl_show'
    
    def __init__(self, show_id = None, name = None, \
                 phone = None, email = None ):
        self._id = show_id
        self.name = name
        
    @property
    def show_id(self):
        return self._id
    
    @show_id.setter
    def show_id(self, value):
        self._id = value

    @property
    def name(self):
        return self._name
    
    @name.setter
    def name(self, value):
        self._name = value
        
    @property
    def producers(self):
        query=(db.tbl_producer_show.show_id == self._id)
        rows = db(query).select(db.tbl_producer_show.producer_id)
        return [Producer(producer_id=row.producer_id) for row in rows]

    # TODO: These two method needs optimization
    # for huge number of clips
    @property
    def clips(self):
        query = (db.tbl_producer_show.show_id == self._id)& \
            (db.tbl_selected_clip.ps_id == db.tbl_producer_show.id)& \
            (db.tbl_clip.id == db.tbl_selected_clip.clip_id)
        rows = db(query).select(db.tbl_clip.id)
        return [Clip(clip_id=row.id) for row in rows]

    def total_clip_duration(self):
        return sum([x.sync().duration for x in self.clips])


if __name__=='__main__':
    import doctest

    def run_all_tests():
        """
        >>> p = Producer(name='Alex')
        >>> p.save(); p.commit()
        >>> c = Clip(name='First Clip')
        >>> c.save(); c.commit()
        >>> s = Show(name='Nice Show')
        >>> s.save(); s.commit()
        >>> db.tbl_producer_show[0] = { 'show_id' : 1, 'producer_id' : 1}
        >>> db.commit()
        >>> db.tbl_selected_clip[0] = { 'ps_id' : 1, 'clip_id' : 1}
        >>> db.commit()
        >>> show = Show(show_id=1).sync()
        >>> print show
        Show_1<Nice Show>
        >>> [x.sync() for x in show.producers]
        [Producer_1<None, Alex, None>]
        >>> [x.sync() for x in show.clips]
        [Clip_1<None, None, First Clip, None>]
        """
        pass

    def testSetup():
        for table in db.tables:
            db[table].truncate()

     
    testSetup()
    doctest.testmod()
    
    ''' Un comment if you want ipython shell
        You will need ipython installed
    '''
    #from IPython import embed as ishell
    #ishell()
        
