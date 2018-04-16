package zk.crawler;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import cn.edu.hfut.dmic.contentextractor.ContentExtractor;
import cn.edu.hfut.dmic.webcollector.model.CrawlDatum;
import cn.edu.hfut.dmic.webcollector.model.CrawlDatums;
import cn.edu.hfut.dmic.webcollector.model.Page;
import cn.edu.hfut.dmic.webcollector.plugin.berkeley.BreadthCrawler;

public class t1 extends BreadthCrawler {

	static volatile boolean flag = false;
	static volatile AtomicInteger rount = new AtomicInteger(0);

	static final Set<String> dictionary = new HashSet<String>();

	static BlockingQueue<String> q = new LinkedBlockingQueue<String>();

	/**
	 * @param crawlPath
	 *            crawlPath is the path of the directory which maintains
	 *            information of this crawler
	 * @param autoParse
	 *            if autoParse is true,BreadthCrawler will auto extract links
	 *            which match regex rules from pag
	 */
	public t1(String crawlPath, boolean autoParse) {
		super(crawlPath, autoParse);

		// http://tougao.12371.cn/liebiao.php?fid=145&digest=1&digest=1&filter=digest&page=1
		// http://tougao.12371.cn/gaojian.php?tid=1196513

		/* start page */
		this.addSeed("http://www.djyj.cn/");

		/* fetch url like http://news.hfut.edu.cn/show-xxxxxxhtml */
		// this.addRegex("http://www.dldj.gov.cn/show.*");

		// this.addRegex("http://www.dldj.gov.cn/trans.*");

		// trans
		// this.addRegex();
		/* do not fetch jpg|png|gif */
		this.addRegex(".*");

		this.addRegex("-.*\\.(jpg|png|gif).*");
		/* do not fetch url contains # */
		this.addRegex("-.*#.*");

		setThreads(50);
		getConf().setTopN(1000);

		// setResumable(true);
	}

	@Override
	protected void afterParse(Page page, CrawlDatums next) {
		// 当前页面的depth为x，则从当前页面解析的后续任务的depth为x+1
		int depth;
		// 如果在添加种子时忘记添加depth信息，可以通过这种方式保证程序不出错
		if (page.meta("depth") == null) {
			depth = 1;
		} else {
			depth = Integer.valueOf(page.meta("depth"));
		}
		depth++;
		for (CrawlDatum datum : next) {
			datum.meta("depth", depth + "");
		}

	}

	public void visit(Page page, CrawlDatums next) {
		String url = page.url();
		int depth;
		// 如果在添加种子时忘记添加depth信息，可以通过这种方式保证程序不出错
		if (page.meta("depth") == null) {
			depth = 1;
		} else {
			depth = Integer.valueOf(page.meta("depth"));
		}
		String parent;
		if (page.meta("parent") == null) {
			parent = "null";
		} else {
			parent = page.meta("parent");
		}
		
		/*
		 * /*if page is news page
		 */
		//
		// http://www.dldj.gov.cn/show.aspx?id=97514&cid=476

		try {
			String content = ContentExtractor.getContentByHtml(page.html());
			// System.out.println("URL:\n" + url);
			// System.out.println("title:\n" + title);
			// System.out.println("content:\n" + content.substring(0, 20));
			q.put(depth+"\n" +"parent:"+parent+"\n"+ url + "\n" + content+"\n");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		/* If you want to add urls to crawl,add them to nextLink */
		/*
		 * WebCollector automatically filters links that have been fetched
		 * before
		 */
		/*
		 * If autoParse is true and the link you add to nextLinks does not match
		 * the regex rules,the link will also been filtered.
		 */
		// next.add("http://xxxxxx.com");
		//

		// 默认已关闭自动解析功能 2018-4-13 15:47 ZK
		// 针对锚文本过滤，如果锚文本含有主题字典中的主题词，则认为此URL时相关的，加入种子集合 2018-4-15 29:25 ZK
		String conteType = page.contentType();
		if (conteType != null && conteType.contains("text/html")) {
			Document doc = page.doc();
			if (doc != null) {
				// System.err.println(regexRule.isEmpty());
				for (Element HrefElement : doc.select("a[href]")) {
					String href = HrefElement.attr("abs:href");// 获取URL绝对地址
					String anchor = HrefElement.text();// 获取锚文本
					// System.err.println(href);
					// System.err.println(anchor);
					if (regexRule.satisfy(href)) {
						for (String topic : dictionary) {
							if (anchor.contains(topic)) {
								CrawlDatum crawdatum = new CrawlDatum(href);
								crawdatum.meta("parent", url);
								next.add(crawdatum);
								break;
							}
						}
					}
				}
			}
		}

	}

	static class Writer implements Runnable {
		String dicpath;
		File file;

		public Writer(String path) {
			dicpath = path;
			file = new File(dicpath);

		}

		public void run() {
			// TODO Auto-generated method stub
			FileWriter w = null;
			try {
				w = new FileWriter(file);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			while (flag) {
				while (!q.isEmpty()) {
					String str = null;

					try {
						str = q.take();
						w.write(str + "\n");
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}

			try {
				w.flush();
				w.close();
				System.err.println("writer close!");
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}

	public static void main(String[] args) throws Exception {
		System.err.println("Notice:the dictionary must be utf-8 without BOM!");
		if (args.length != 5) {
			System.out.println("Usage: crawlPath autoParse round coreDictionary outputFilepath");
			System.exit(0);
		}
		String crawlPath = null;
		boolean autoParse = true;
		int round = 0;
		String coreDictionary = null;
		String outputFilepath = null;
		try {
			crawlPath = args[0];
			autoParse = Boolean.parseBoolean(args[1]);
			round = Integer.parseInt(args[2]);
			coreDictionary = args[3];
			outputFilepath = args[4];
		} catch (Exception e) {
			System.out.println("args error! check it!");
			System.exit(0);
		}
		autoParse = false;// 目前锁定不开启自动解析URL，为了实现字典过滤 2018-4-13 15:37
		BufferedReader bf = new BufferedReader(new FileReader(new File(coreDictionary)));
		String str = "";
		while ((str = bf.readLine()) != null) {
			dictionary.add(str);
		}
		bf.close();
		// System.out.println(dictionary);
		flag = true;
		Writer writer = new Writer(outputFilepath);
		Thread writethread = new Thread(writer);
		writethread.start();
		t1 crawler = new t1(crawlPath, autoParse);
		/* start crawl with depth of round */
		crawler.setMaxExecuteCount(50);
		crawler.start(round);
		flag = false;
		if (!q.isEmpty()) {
			FileWriter wlast = new FileWriter(new File(outputFilepath), true);
			while (!q.isEmpty()) {
				str = q.poll();
				if (str == null) {
					break;
				}
				wlast.write(str);
			}
			wlast.close();
		}
		System.err.println("crawler close!");
	}

}
