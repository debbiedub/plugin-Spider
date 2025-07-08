
package plugins.Spider;

import freenet.keys.FreenetURI;
import freenet.pluginmanager.PluginRespirator;
import freenet.support.Logger;
import freenet.support.SimpleFieldSet;
import freenet.support.api.Bucket;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.util.Collection;
import java.util.TreeMap;
import plugins.Spider.index.TermEntryWriter;
import plugins.Spider.index.TermPageEntry;
import plugins.Spider.db.Status;
import plugins.Spider.org.garret.perst.Storage;

/**
 * Buffer which stores TermPageEntrys as they are found by the Spider. When the
 * buffer's estimated size gets up to bufferMax, the buffer is serialized into a
 * Bucket and sent to the Library
 * 
 *
 * @author MikeB
 */
public class LibraryBuffer {
	private PluginRespirator pr;
	private long timeStalled = 0;
	private long timeNotStalled = 0;
	private long timeLastNotStalled = System.currentTimeMillis();
	private boolean shutdown;
	private FreenetURI lastURI;
	private boolean badURI;

	private TreeMap<TermPageEntry, TermPageEntry> termPageBuffer = new TreeMap();
	// Garbage collection behaving perversely. Lets try moving stuff into instance members.
	private Collection<TermPageEntry> pushing = null;

	private int bufferUsageEstimate = 0;
	private int bufferMax;

	static final File SAVE_FILE = new File("spider.saved.data");
	
	/** For resetPages */
	private Spider spider;

	synchronized void setBufferSize(int maxSize) {
		if(maxSize <= 0) throw new IllegalArgumentException();
		bufferMax = maxSize;
	}

	/** We always write after a file has been successfully processed. */
	public void alwaysWrite() {
		int bufferUsageEstimated = 0;
		synchronized(this) {
			if(pushing != null) {
				throw new IllegalStateException("Still pushing?!");
			}
			pushing = termPageBuffer.values();
			termPageBuffer = new TreeMap();
			bufferUsageEstimated = bufferUsageEstimate;
			bufferUsageEstimate = 0;
			writeFile(bufferUsageEstimated);
		}
	}

	/**
	 * Increments the estimate by specified amount.
	 * @param increment
	 */
	private synchronized void increaseEstimate(int increment) {
		bufferUsageEstimate += increment;
	}
	
	public synchronized int bufferUsageEstimate() {
		return bufferUsageEstimate;
	}
	

	LibraryBuffer(PluginRespirator pr, Spider spider) {
		this.pr = pr;
		this.spider = spider;
	}
	
	public void start() {
		// Do in a transaction so it gets committed separately.
		spider.db.beginThreadTransaction(Storage.EXCLUSIVE_TRANSACTION);
		spider.resetPages(Status.NOT_PUSHED, Status.NEW);
		spider.db.endThreadTransaction();
	}

	/**
	 * Takes a TermPageEntry and either returns a TPE of the same term & page
	 * from the buffer or adds the TPE to the buffer and returns it.
	 *
	 * @param newTPE
	 * @return
	 */
	private synchronized TermPageEntry get(TermPageEntry newTPE) {
		if(shutdown) {
			while(true)
				try {
					wait();// Don't add anything more, don't allow the transaction to commit.
					// FIXME throw something instead???
				} catch (InterruptedException e) {
					// Ignore
				} 
		}
		TermPageEntry exTPE = termPageBuffer.get(newTPE);
		if(exTPE==null) {	// TPE is new
			increaseEstimate(newTPE.sizeEstimate());
			termPageBuffer.put(newTPE, newTPE);
			return newTPE;
		} else
			return exTPE;
	}

	/**
	 * Set the title of the
	 * @param termPageEntry
	 * @param s
	 */
	synchronized void setTitle(TermPageEntry termPageEntry, String s) {
		get(termPageEntry).title = s;
	}
	
	synchronized void setRelevance(TermPageEntry termPageEntry, float f) {
		get(termPageEntry).rel = f;
	}

	/**
	 * Puts a term position in the TermPageEntry and increments the bufferUsageEstimate
	 * @param tp
	 * @param position
	 */
	synchronized void addPos(TermPageEntry tp, int position) {
		//Logger.normal(this, "length : "+bufferUsageEstimate+", adding to "+tp);
		get(tp).putPosition(position);
		//Logger.normal(this, "length : "+bufferUsageEstimate+", increasing length "+tp);
		increaseEstimate(4);
	}

	/**
	 * Emptys the buffer into a bucket and sends it to the Library plugin with the command "pushBuffer"
	 *
	 * FIXME : I think there is something wrong with the way it writes to the bucket, I may be using the wrong kind of buffer
	 */
	private void writeFile(int bufferUsageEstimated) {
		long tStart = System.currentTimeMillis();
		try {
			int fileNumber = 0;
			File file;
			do {
				file = new File(".", "spider.index.data." + ++fileNumber);
			} while (file.exists());
			Logger.normal(this, "Writing file of estimated size " + bufferUsageEstimated + " bytes to Library in file " + file);
			OutputStream os = new FileOutputStream(file);
			writeToPush(totalPagesIndexed(), os);
			Logger.normal(this, "File successfully written to Library");
			// Not a separate transaction, commit with the index updates.
			spider.donePages();
		} catch (IOException ex) {
			Logger.error(this, "Could not make bucket to transfer buffer", ex);
		}
		long tEnd = System.currentTimeMillis();
		synchronized(this) {
			timeNotStalled += (tStart - timeLastNotStalled);
			timeLastNotStalled = tEnd;
			timeStalled += (tEnd - tStart);
			if(shutdown) return;
		}
	}

	private long totalPagesIndexed() {
		return spider.getRoot().getPageCount(Status.DONE)
				+ spider.getRoot().getPageCount(Status.PROCESSED_KSK)
				+ spider.getRoot().getPageCount(Status.PROCESSED_USK);
	}
	
	private synchronized void writeToPush(long totalPagesIndexed, OutputStream os) throws IOException {
		SimpleFieldSet meta = new SimpleFieldSet(true); // Stored with data to make things easier.
		String indexTitle = spider.getConfig().getIndexTitle();
		String indexOwner = spider.getConfig().getIndexOwner();
		String indexOwnerEmail = spider.getConfig().getIndexOwnerEmail();
		if(indexTitle != null)
			meta.putSingle("index.title", indexTitle);
		if(indexOwner != null)
			meta.putSingle("index.owner.name", indexOwner);
		if(indexOwnerEmail != null)
			meta.putSingle("index.owner.email", indexOwnerEmail);
		meta.put("totalPages", totalPagesIndexed);
		meta.writeTo(os);
		for (TermPageEntry termPageEntry : pushing) {
			TermEntryWriter.getInstance().writeObject(termPageEntry, os);
		}
		pushing = null;
		os.close();
	}

	public long getTimeStalled() {
		return timeStalled;
	}
	
	public long getTimeNotStalled() {
		return timeNotStalled;
	}

	public void terminate() {
		synchronized(this) {
			if(shutdown) {
				Logger.error(this, "Shutdown called twice", new Exception("error"));
				return;
			}
			shutdown = true;
			pushing = termPageBuffer.values();
			termPageBuffer = new TreeMap();
			bufferUsageEstimate = 0;
		}
	}

	/** Reads the (first) line from a file and returns as a String.
	 * If there is a problem null is returned.
	 */
	static String readFileLine(final String filename) {
		File f = new File(filename);
		FileInputStream fis;
		try {
			fis = new FileInputStream(f);
		} catch (FileNotFoundException e) {
			return null;
		}
		BufferedReader br = null;
		String line;
		try {
			br = new BufferedReader(new InputStreamReader(fis, "UTF-8"));
			line = br.readLine();
		} catch (UnsupportedEncodingException e) {
			return null;
		} catch (IOException e) {
			return null;
		} finally {
			try {
				if (br != null) {
					br.close();
				}
			} catch (IOException e) {
				// Ignore the problem.
			}
		}
		return line;
	}

	public FreenetURI getURI() {
		String ssk = readFileLine("library.index.pubkey");
		String edition = readFileLine("library.index.last-edition");
		if (ssk != null && edition != null) {
			String keyPart = ssk.substring("freenet:SSK@".length());

			try {
				return new FreenetURI("USK@" + keyPart + "/" + edition);
			} catch (MalformedURLException e) {
				return null;
			}
		} else {
			return null;
		}
	}
	
}
