package ohdm2mapnik;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TagsTest {

    @Test
    void testCleanupTags() {
        // test remove prefix
        Tags tags = new Tags("\"note:ref\"=>\"56\", \"wikidata\"=>\"Q4472254\"");
        tags.cleanupTags();
        assertEquals("\"ref\"=>\"56\", \"wikidata\"=>\"Q4472254\"", tags.getHstoreTags());

        // test remove tag
        tags = new Tags("\"note\"=>\"56\", \"wikidata\"=>\"Q4472254\"");
        tags.cleanupTags();
        assertEquals("\"wikidata\"=>\"Q4472254\"", tags.getHstoreTags());
    }

    @Test
    void testGetHstoreTags() {
        // test without values
        Tags tags = new Tags("");
        assertEquals("", tags.getHstoreTags());

        tags = new Tags("null");
        assertEquals("", tags.getHstoreTags());

        // test with values
        tags = new Tags("\"ref\"=>\"56\", \"wikidata\"=>\"Q4472254\"");
        assertEquals("\"ref\"=>\"56\", \"wikidata\"=>\"Q4472254\"", tags.getHstoreTags());

        tags = new Tags("\"ref\"=>\"56\"");
        assertEquals("\"ref\"=>\"56\"", tags.getHstoreTags());
    }

}
