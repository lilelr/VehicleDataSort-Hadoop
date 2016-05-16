package sort;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
public class  FilesList
{
	public static Path[] getList(Configuration conf, String uriPath, String hdfsPath, String fileName) throws URISyntaxException, IOException
 	{
	URI uri = new URI(uriPath); 
	FileSystem fs = FileSystem.get(uri, conf);
	FileStatus[] status = fs.listStatus(new Path(uriPath + hdfsPath.substring(1)));
	ArrayList<Path> r = new ArrayList<Path>(32);
	for (FileStatus file : status)
	{
		String path = file.getPath().toString();
		if (!path.endsWith("/")) path += "/";
		path += fileName;
		Path p = new Path(path);
		if (fs.exists(p))
		r.add(p);
	}
	return r.toArray(new Path[r.size()]);
//	System.out.println(file.getPath().toString());
	}
}