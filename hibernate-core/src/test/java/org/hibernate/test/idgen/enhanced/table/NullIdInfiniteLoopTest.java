package org.hibernate.test.idgen.enhanced.table;

import org.junit.Test;

import org.hibernate.Session;
import org.hibernate.id.enhanced.TableGenerator;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.testing.junit4.BaseCoreFunctionalTestCase;

import static org.hibernate.id.IdentifierGeneratorHelper.BasicHolder;
import static org.hibernate.testing.junit4.ExtraAssertions.assertClassAssignability;
import static org.junit.Assert.assertEquals;

public class NullIdInfiniteLoopTest extends BaseCoreFunctionalTestCase {

    @Override
    public String[] getMappings() {
        return new String[] { "idgen/enhanced/table/Basic.hbm.xml" };
    }

    @Test
    public void testNormalBoundary() throws InterruptedException {
        EntityPersister persister = sessionFactory().getMetamodel().entityPersister( Entity.class.getName() );
        assertClassAssignability( TableGenerator.class, persister.getIdentifierGenerator().getClass() );
        TableGenerator generator = ( TableGenerator ) persister.getIdentifierGenerator();

        int count = 5;
        Entity[] entities = new Entity[count];
        Session s = openSession();

        // This situation can only happen through a bad row inserted via
        // human being or migration/cloning operation. Simulate this
        // action post table generation.
        s.beginTransaction();
        s.createNativeQuery(
            "UPDATE ID_TBL_BSC_TBL SET next_val = null where sequence_name = 'test'"
        ).executeUpdate();
        s.getTransaction().commit();

        // Failure will result in infinite loop
        s.beginTransaction();
        for (int i = 0; i < count; i++) {
            entities[i] = new Entity("" + (i + 1));
            s.save(entities[i]);
            long expectedId = i + 1;
            assertEquals(expectedId, entities[i].getId().longValue());
            assertEquals(expectedId, generator.getTableAccessCount());
            assertEquals(expectedId, ((BasicHolder) generator.getOptimizer().getLastSourceValue()).getActualLongValue());
        }
        s.getTransaction().commit();

        s.beginTransaction();
        for ( int i = 0; i < count; i++ ) {
            assertEquals( i + 1, entities[i].getId().intValue() );
            s.delete( entities[i] );
        }
        s.getTransaction().commit();
        s.close();
    }

}
