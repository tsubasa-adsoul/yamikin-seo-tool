import streamlit as st
import os  # ← これを追加
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import plotly.express as px
import plotly.graph_objects as go
import os
import json
from google.oauth2 import service_account
from googleapiclient.discovery import build
import google.generativeai as genai
import requests
from bs4 import BeautifulSoup
import time

class SEOAnalyzerStreamlit:
    def __init__(self):
        """Streamlit版SEO分析ツール初期化"""
        self.config = self.load_local_config()
        
        self.credentials_file = self.config.get('credentials_file', 'credentials/gemini-analysis-467706-b3196e5deffe.json')
        self.gemini_api_key = st.secrets.get('gemini_api_key', self.config.get('gemini_api_key', ''))

        
        self.scopes = [
            'https://www.googleapis.com/auth/webmasters.readonly',
            'https://www.googleapis.com/auth/analytics.readonly',
            'https://www.googleapis.com/auth/spreadsheets.readonly',
            'https://www.googleapis.com/auth/drive'
        ]
        
        # 認証初期化
        self.credentials = self.load_credentials()
        self.gsc_service = None
        self.ga4_service = None
        
        # Gemini初期化
        self.gemini_model = None
        if self.gemini_api_key:
            try:
                genai.configure(api_key=self.gemini_api_key)
                self.gemini_model = genai.GenerativeModel('gemini-1.5-flash')
            except Exception as e:
                st.error(f"Gemini API初期化エラー: {e}")

    
    def load_config_from_spreadsheet(self):
        """スプレッドシートから設定を読み込む"""
        try:
            loader = SpreadsheetConfigLoader()
            sites = loader.load_sites_from_spreadsheet()
            
            if sites:
                config = loader.create_config_with_sites(sites)
                
                # ローカルのconfig.jsonからAPI関連の設定を読み込んで追加
                local_config = self.load_local_config()
                config['google_api_key'] = st.secrets.get('google_api_key', local_config.get('google_api_key', ''))
                config['gemini_api_key'] = st.secrets.get('gemini_api_key', local_config.get('gemini_api_key', ''))
                config['search_engine_id'] = local_config.get('search_engine_id', '734633bb3016e4892')
                
                return config
        except Exception as e:
            st.error(f"スプレッドシート読み込みエラー: {e}")
        return None

    
    def load_local_config(self):
        """ローカル設定ファイル読み込み"""
        if os.path.exists('config.json'):
            with open('config.json', 'r', encoding='utf-8') as f:
                config = json.load(f)
                # 検索エンジンIDを追加
                config['search_engine_id'] = '734633bb3016e4892'
                return config
        return {'search_engine_id': '734633bb3016e4892'}
    
    def load_credentials(self):
        """認証情報読み込み"""
        try:
            # デバッグ情報
            st.info("認証情報を読み込み中...")
            
            # Secretsから読み込み
            if 'gcp_service_account' in st.secrets:
                st.info("Secretsから認証情報を取得")
                from google.oauth2 import service_account
                
                # Secretsから認証情報を構築
                credentials_dict = dict(st.secrets["gcp_service_account"])
                
                # private_keyの改行を修正
                if 'private_key' in credentials_dict:
                    credentials_dict['private_key'] = credentials_dict['private_key'].replace('\\n', '\n')
                
                credentials = service_account.Credentials.from_service_account_info(
                    credentials_dict, scopes=self.scopes
                )
                return credentials
            else:
                st.warning("Secretsに認証情報がありません。ローカルファイルを使用します。")
                # ローカルファイルから読み込み（フォールバック）
                credentials = service_account.Credentials.from_service_account_file(
                    self.credentials_file, scopes=self.scopes
                )
                return credentials
        except Exception as e:
            st.error(f"認証エラー: {e}")
            import traceback
            st.error(traceback.format_exc())
            return None


    
    def init_services(self):
        """APIサービス初期化"""
        if not self.credentials:
            return False
        try:
            self.gsc_service = build('searchconsole', 'v1', credentials=self.credentials)
            self.ga4_service = build('analyticsdata', 'v1beta', credentials=self.credentials)
            return True
        except Exception as e:
            st.error(f"サービス初期化エラー: {e}")
            return False

    
    def get_gsc_data(self, site_url, current_start, current_end, comparison_start, comparison_end):
        """GSCデータ取得（メモリ効率改善版）"""
        if not self.gsc_service:
            if not self.init_services():
                return None, None
        
        # デバッグ情報を追加
        st.write(f"GSC URL: {site_url}")
        st.write(f"期間: {current_start} 〜 {current_end}")
        
        # キャッシュキーを生成
        cache_key = f"{site_url}_{current_start}_{current_end}"
        is_first_time = 'data_cache' not in st.session_state or cache_key not in st.session_state.data_cache

        
        try:
            # デバッグ：実際にAPIを呼び出しているか確認
            st.info(f"GSC API呼び出し中: {site_url}")
            
            # 初回のみプログレスバーを表示
            if is_first_time:
                progress_text = "GSCデータ取得中..."
                progress_bar = st.progress(0, text=progress_text)
                progress_bar.progress(25, text="現在期間データ取得中...")

            
            # 現在期間データ取得
            current_request = {
                'startDate': current_start.strftime('%Y-%m-%d'),
                'endDate': current_end.strftime('%Y-%m-%d'),
                'dimensions': ['query', 'page'],
                'rowLimit': 1000
            }
            
            current_response = self.gsc_service.searchanalytics().query(
                siteUrl=site_url, body=current_request
            ).execute()
            
            # デバッグ：レスポンスを確認
            st.write(f"現在期間のデータ件数: {len(current_response.get('rows', []))}")

            
            # 比較期間データ取得
            if is_first_time:
                progress_bar.progress(50, text="比較期間データ取得中...")
            
            comparison_request = {
                'startDate': comparison_start.strftime('%Y-%m-%d'),
                'endDate': comparison_end.strftime('%Y-%m-%d'),
                'dimensions': ['query', 'page'],
                'rowLimit': 1000
            }
            
            comparison_response = self.gsc_service.searchanalytics().query(
                siteUrl=site_url, body=comparison_request
            ).execute()
            
            # DataFrame作成
            if is_first_time:
                progress_bar.progress(75, text="データ処理中...")
            
            current_df = self.gsc_to_dataframe(current_response)
            comparison_df = self.gsc_to_dataframe(comparison_response)
            
            if is_first_time:
                progress_bar.progress(100, text="完了！")
                time.sleep(0.5)
                progress_bar.empty()
            
            return current_df, comparison_df
            
        except Exception as e:
            st.error(f"GSCデータ取得エラー: {e}")
            st.error(f"詳細: {type(e).__name__}")  # 追加
            import traceback
            st.error(traceback.format_exc())  # 追加
            return None, None





    def save_analysis_result(self, keyword, url, analysis, mode):
        """分析結果を保存（スプレッドシート優先）"""
        
        # ローカル保存（フォールバック）
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        safe_keyword = keyword.replace(' ', '_').replace('/', '_')
        site_name = st.session_state.site['name'].replace(' ', '_')
        user_name = os.environ.get('USERNAME', 'unknown')
        filename = f"{site_name}_{timestamp}_{user_name}_{safe_keyword}.json"
        
        os.makedirs("analysis_log", exist_ok=True)
        
        data = {
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "keyword": keyword,
            "url": url,
            "analysis": analysis,
            "mode": mode,
            "user": user_name,
            "site": st.session_state.site['name'] if 'site' in st.session_state else 'unknown'
        }
        
        with open(f"analysis_log/{filename}", 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        
        return f"ローカル保存: {filename}"

    
    def load_analysis_history(self, site_name=None, limit=20):
        """履歴を読み込み（ローカルのみ）"""
        if not os.path.exists("analysis_log"):
            return []
        
        files = sorted(os.listdir("analysis_log"), reverse=True)
        history = []
        
        for file in files:
            if file.endswith('.json'):
                try:
                    with open(f"analysis_log/{file}", 'r', encoding='utf-8') as f:
                        data = json.load(f)
                        if site_name is None or data.get('site') == site_name:
                            history.append(data)
                            if len(history) >= limit:
                                break
                except:
                    continue
        
        return history




    
    def gsc_to_dataframe(self, response):
        """GSCレスポンスをDataFrameに変換"""
        rows = response.get('rows', [])
        data = []
        
        for row in rows:
            query = row['keys'][0]
            page = row['keys'][1]
            clicks = row.get('clicks', 0)
            impressions = row.get('impressions', 0)
            ctr = row.get('ctr', 0)
            position = row.get('position', 0)
            
            data.append({
                'query': query,
                'page': page,
                'clicks': clicks,
                'impressions': impressions,
                'ctr': ctr,
                'position': position
            })
        
        return pd.DataFrame(data)
    
    def get_ga4_data(self, property_id, start_date, end_date):
        """GA4データ取得（日付直接指定版）"""
        # GA4未設定時は空のDataFrameを返す（エラー表示なし）
        if not property_id or property_id == "":
            return pd.DataFrame()
            
        if not self.ga4_service:
            if not self.init_services():
                return pd.DataFrame()  # Noneではなく空のDataFrameを返す

        
        try:
            # リクエストボディを直接辞書で作成
            request_body = {
                "dateRanges": [{
                    "startDate": start_date.strftime('%Y-%m-%d'),
                    "endDate": end_date.strftime('%Y-%m-%d')
                }],
                "dimensions": [
                    {"name": "pagePath"},
                    {"name": "sessionSourceMedium"}
                ],
                "metrics": [
                    {"name": "sessions"},
                    {"name": "totalUsers"},
                    {"name": "bounceRate"},
                    {"name": "averageSessionDuration"},
                    {"name": "conversions"}
                ]
            }
            
            response = self.ga4_service.properties().runReport(
                property=f"properties/{property_id}",
                body=request_body
            ).execute()
            
            return self.ga4_to_dataframe(response)
            
        except Exception as e:
            st.error(f"GA4データ取得エラー: {e}")
            return None

    
    def ga4_to_dataframe(self, response):
        """GA4レスポンスをDataFrameに変換"""
        rows = response.get('rows', [])
        data = []
        
        for row in rows:
            page_path = row['dimensionValues'][0]['value']
            source_medium = row['dimensionValues'][1]['value']
            sessions = int(row['metricValues'][0]['value'])
            users = int(row['metricValues'][1]['value'])
            bounce_rate = float(row['metricValues'][2]['value'])
            avg_duration = float(row['metricValues'][3]['value'])
            conversions = int(row['metricValues'][4]['value'])
            
            if 'organic' in source_medium.lower():
                data.append({
                    'page_path': page_path,
                    'source_medium': source_medium,
                    'sessions': sessions,
                    'users': users,
                    'bounce_rate': bounce_rate,
                    'avg_duration': avg_duration,
                    'conversions': conversions,
                    'cvr': conversions / sessions if sessions > 0 else 0
                })
        
        return pd.DataFrame(data)
    
    def analyze_trends(self, current_df, comparison_df, change_threshold=50, min_clicks=5):
        """トレンド分析"""
        # クエリ別集計
        current_summary = current_df.groupby('query').agg({
            'clicks': 'sum',
            'impressions': 'sum',
            'ctr': 'mean',
            'position': 'mean',
            'page': 'first'
        }).reset_index()
        
        comparison_summary = comparison_df.groupby('query').agg({
            'clicks': 'sum',
            'impressions': 'sum',
            'ctr': 'mean',
            'position': 'mean'
        }).reset_index()
        
        # マージして変化率計算
        merged = pd.merge(
            current_summary, comparison_summary, 
            on='query', suffixes=('_current', '_comparison'), how='outer'
        ).fillna(0)
        
        # 変化率計算
        merged['clicks_change'] = merged['clicks_current'] - merged['clicks_comparison']
        
        # 変化率の計算を改善
        def calculate_change_rate(row):
            if row['clicks_comparison'] == 0:
                if row['clicks_current'] > 0:
                    return "新規"
                else:
                    return 0
            else:
                return (row['clicks_change'] / row['clicks_comparison'] * 100)
        
        merged['clicks_change_rate'] = merged.apply(calculate_change_rate, axis=1)
        
        # 大幅変化キーワード抽出
        def is_significant(row):
            if row['clicks_change_rate'] == "新規":
                return row['clicks_current'] >= min_clicks
            else:
                return (abs(row['clicks_change_rate']) >= change_threshold) and (row['clicks_current'] >= min_clicks)
        
        significant_changes = merged[merged.apply(is_significant, axis=1)].sort_values('clicks_change', ascending=False)
        
        # 列名変更
        significant_changes = significant_changes.rename(columns={
            'query': '検索キーワード',
            'page': 'ページURL',
            'clicks_current': '現在期間_クリック数',
            'impressions_current': '現在期間_表示回数',
            'ctr_current': '現在期間_CTR',
            'position_current': '現在期間_平均順位',
            'clicks_comparison': '比較期間_クリック数',
            'impressions_comparison': '比較期間_表示回数',
            'ctr_comparison': '比較期間_CTR',
            'position_comparison': '比較期間_平均順位',
            'clicks_change': 'クリック数変化',
            'clicks_change_rate': '変化率(%)'
        })
        
        return significant_changes

    
    def analyze_performance(self, current_df, comparison_df):
        """パフォーマンス比較分析"""
        current_total = {
            'period': '現在期間',
            'total_clicks': current_df['clicks'].sum(),
            'total_impressions': current_df['impressions'].sum(),
            'avg_ctr': current_df['ctr'].mean() * 100,
            'avg_position': current_df['position'].mean(),
            'unique_queries': current_df['query'].nunique()
        }
        
        comparison_total = {
            'period': '比較期間',
            'total_clicks': comparison_df['clicks'].sum(),
            'total_impressions': comparison_df['impressions'].sum(),
            'avg_ctr': comparison_df['ctr'].mean() * 100,
            'avg_position': comparison_df['position'].mean(),
            'unique_queries': comparison_df['query'].nunique()
        }
        
        return pd.DataFrame([current_total, comparison_total])
    
    def analyze_conversion(self, ga4_df):
        """コンバージョン分析"""
        if ga4_df is None or len(ga4_df) == 0:
            return pd.DataFrame()
        
        # CVR順でソート
        cv_analysis = ga4_df[ga4_df['sessions'] >= 10].sort_values('cvr', ascending=False)
        
        # 列名変更
        cv_analysis = cv_analysis.rename(columns={
            'page_path': 'ページパス',
            'source_medium': '流入元',
            'sessions': 'セッション数',
            'users': 'ユーザー数',
            'bounce_rate': '直帰率',
            'avg_duration': '平均セッション時間',
            'conversions': 'コンバージョン数',
            'cvr': 'CVR(%)'
        })
        
        return cv_analysis
    
    def analyze_search_intent(self, current_df, ctr_threshold=0.05, min_impressions=100):
        """検索意図分析"""
        # ページごとに集計
        page_summary = current_df.groupby(['query', 'page']).agg({
            'clicks': 'sum',
            'impressions': 'sum',
            'ctr': 'mean',
            'position': 'mean'
        }).reset_index()
        
        # CTR低い×インプレッション多い
        low_ctr_high_imp = page_summary[
            (page_summary['impressions'] >= min_impressions) & 
            (page_summary['ctr'] < ctr_threshold)
        ].sort_values('impressions', ascending=False)
        
        # 列名変更
        low_ctr_high_imp = low_ctr_high_imp.rename(columns={
            'query': '検索キーワード',
            'page': 'ページURL',
            'clicks': 'クリック数',
            'impressions': '表示回数',
            'ctr': 'CTR',
            'position': '平均掲載順位'
        })
        
        low_ctr_high_imp['CTR'] = low_ctr_high_imp['CTR'] * 100
        
        return low_ctr_high_imp
    
    def fetch_article_content(self, url, base_domain):
        """記事内容を取得"""
        try:
            if not url.startswith('http'):
                url = base_domain.rstrip('/') + url
            
            response = requests.get(url, timeout=10)
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # タイトル、見出し、本文を抽出
            title = soup.find('title').text if soup.find('title') else ''
            h1 = soup.find('h1').text if soup.find('h1') else ''
            h2_list = [h2.text.strip() for h2 in soup.find_all('h2')][:5]
            
            # メインコンテンツ
            main_content = (
                soup.find('main') or 
                soup.find('article') or 
                soup.find('div', class_='content')
            )
            
            if main_content:
                text = main_content.get_text(separator=' ', strip=True)[:2000]
            else:
                text = soup.get_text(separator=' ', strip=True)[:2000]
            
            return {
                'title': title,
                'h1': h1,
                'h2_list': h2_list,
                'content_preview': text,
                'success': True
            }
            
        except Exception as e:
            return {
                'error': str(e),
                'success': False
            }
    
    def analyze_article_with_ai(self, keyword, url, content, metrics):
        """AI による記事分析"""
        if not self.gemini_model:
            return "Gemini APIが設定されていません"
        
        try:
            prompt = f"""
            SEO専門家として、以下の記事を分析し、検索順位を上げるための具体的な改善提案をしてください。

            【検索キーワード】{keyword}
            【URL】{url}
            
            【現在のパフォーマンス】
            - クリック数: {metrics.get('clicks', 'N/A')}
            - 表示回数: {metrics.get('impressions', 'N/A')}
            - CTR: {metrics.get('ctr', 'N/A')}%
            - 平均順位: {metrics.get('position', 'N/A')}

            【現在の記事構成】
            タイトル: {content.get('title', 'なし')}
            H1: {content.get('h1', 'なし')}
            H2見出し: {', '.join(content.get('h2_list', []))}

            【本文プレビュー】
            {content.get('content_preview', '')[:1000]}

            【分析してください】
            1. この記事が検索意図を満たしているか評価（具体的に）
            2. 競合と比較して不足している要素（箇条書き5つ）
            3. タイトルタグの改善案（3つ提示）
            4. 追加すべきコンテンツセクション（具体的に）
            5. 内部リンクの設置提案
            6. 今すぐ実行できる改善アクション（優先順位付き3つ）

            簡潔で実行可能な提案をお願いします。
            """
            
            response = self.gemini_model.generate_content(prompt)
            return response.text
            
        except Exception as e:
            return f"AI分析エラー: {str(e)}"
    
    def search_competitors_google(self, keyword, num_results=5):
        """Google Custom Search APIで競合検索"""
        
        # 初期化
        if 'daily_queries' not in st.session_state:
            st.session_state.daily_queries = 0
        if 'query_date' not in st.session_state:
            st.session_state.query_date = datetime.now().date()
        
        # 日付が変わったらリセット
        if st.session_state.query_date != datetime.now().date():
            st.session_state.daily_queries = 0
            st.session_state.query_date = datetime.now().date()


        
        # 使用量チェック
        if st.session_state.daily_queries >= 100:
            if not st.session_state.get('payment_approved', False):
                st.error("❌ Google検索APIの無料枠（100クエリ/日）を超過しています")
                with st.expander("課金を承認する"):
                    st.warning("追加料金: $0.005/クエリ（約0.75円）")
                    if st.button("課金を承認して続行"):
                        st.session_state.payment_approved = True
                        st.rerun()
                return []
        
        try:
            # Google API キーを使用
            google_api_key = self.config.get('google_api_key')
            if not google_api_key:
                st.error("Google APIキーが設定されていません。config.jsonに追加してください。")
                return []
            
            # Custom Search APIを使用
            service = build('customsearch', 'v1', developerKey=google_api_key)
            
            result = service.cse().list(
                q=keyword,
                cx=self.config.get('search_engine_id', '734633bb3016e4892'),
                num=num_results,
                gl='jp',
                lr='lang_ja'
            ).execute()
            
            # 使用量をカウント
            st.session_state.daily_queries += 1
            
            competitors = []
            for i, item in enumerate(result.get('items', [])):
                competitors.append({
                    'rank': i + 1,
                    'url': item['link'],
                    'title': item['title'],
                    'snippet': item.get('snippet', '')
                })
            
            return competitors
            
        except Exception as e:
            st.error(f"Google検索エラー: {e}")
            return []
    
    def analyze_competitor_content(self, url):
        """競合サイトのコンテンツを分析"""
        try:
            response = requests.get(url, timeout=10, headers={
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            })
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # タイトル、見出し構造を取得
            title = soup.find('title').text if soup.find('title') else ''
            h1 = soup.find('h1').text if soup.find('h1') else ''
            h2_list = [h2.text.strip() for h2 in soup.find_all('h2')][:10]
            h3_list = [h3.text.strip() for h3 in soup.find_all('h3')][:10]
            
            # 文字数カウント
            main_content = soup.find('main') or soup.find('article') or soup.find('body')
            word_count = len(main_content.get_text()) if main_content else 0
            
            # 画像数
            images = soup.find_all('img')
            image_count = len(images)
            
            # 内部リンク数
            internal_links = [a for a in soup.find_all('a', href=True) 
                            if a['href'].startswith('/') or url in a['href']]
            
            return {
                'success': True,
                'title': title,
                'h1': h1,
                'h2_list': h2_list,
                'h3_list': h3_list,
                'word_count': word_count,
                'image_count': image_count,
                'internal_link_count': len(internal_links),
                'h2_count': len(h2_list),
                'h3_count': len(h3_list)
            }
        except Exception as e:
            return {
                'success': False,
                'error': str(e)
            }
    
    def compare_with_competitors(self, keyword, my_url, my_content):
        """自サイトと競合サイトを比較分析"""
        with st.spinner(f"「{keyword}」で検索中..."):
            competitors = self.search_competitors_google(keyword, num_results=5)
        
        if not competitors:
            return None
        
        # 競合分析結果を格納
        competitor_analyses = []
        my_site_found = False
        my_rank = None
        
        with st.spinner("競合サイトを分析中..."):
            progress_bar = st.progress(0)
            
            for i, comp in enumerate(competitors):
                progress_bar.progress((i + 1) / len(competitors))
                
                # 自サイトかチェック
                if my_url in comp['url'] or comp['url'] in my_url:
                    my_site_found = True
                    my_rank = comp['rank']
                    continue
                
                # 競合コンテンツ分析
                comp_content = self.analyze_competitor_content(comp['url'])
                if comp_content['success']:
                    competitor_analyses.append({
                        **comp,
                        **comp_content
                    })
                
                time.sleep(1)  # レート制限
        
        # 比較分析をAIで実行
        return self.generate_competitive_analysis(
            keyword, my_content, competitor_analyses, my_rank
        )
    
    def generate_competitive_analysis(self, keyword, my_content, competitors, my_rank):
        """AIによる競合比較分析"""
        if not self.gemini_model:
            return None
        
        # 競合サイトの平均値を計算
        avg_word_count = sum(c['word_count'] for c in competitors) / len(competitors) if competitors else 0
        avg_h2_count = sum(c['h2_count'] for c in competitors) / len(competitors) if competitors else 0
        avg_image_count = sum(c['image_count'] for c in competitors) / len(competitors) if competitors else 0
        
        # 競合の見出し構造をまとめる
        all_h2s = []
        for comp in competitors[:3]:  # 上位3サイト
            all_h2s.extend(comp['h2_list'])
        
        prompt = f"""
        SEO専門家として、検索キーワード「{keyword}」における競合分析を行います。

        【自サイトの状況】
        - 現在の順位: {my_rank if my_rank else '圏外（5位以下）'}
        - タイトル: {my_content.get('title', '')}
        - H1: {my_content.get('h1', '')}
        - H2見出し: {', '.join(my_content.get('h2_list', [])[:5])}
        - 文字数: 約{len(my_content.get('content_preview', ''))}文字

        【競合サイト（上位{len(competitors)}サイト）の傾向】
        - 平均文字数: {avg_word_count:.0f}文字
        - 平均H2数: {avg_h2_count:.0f}個
        - 平均画像数: {avg_image_count:.0f}枚
        
        【上位サイトの見出し傾向】
        {', '.join(set(all_h2s))}

        【分析してください】
        
        1. **コンテンツギャップ分析**
           - 競合にあって自サイトにない重要な要素
           - 追加すべきコンテンツセクション（具体的に）
        
        2. **差別化ポイント**
           - 競合と差別化できる独自の価値提案
           - 追加できるオリジナルコンテンツ
        
        3. **構造的改善点**
           - 見出し構造の最適化案
           - コンテンツボリュームの適正化
        
        4. **即効性のある改善アクション**
           - 今すぐ実施できる3つの改善点
           - 期待される効果
        
        5. **中長期的な戦略**
           - 3ヶ月以内に実施すべき施策
           - 競合を上回るための戦略

        具体的で実行可能な提案をお願いします。
        """
        
        try:
            response = self.gemini_model.generate_content(prompt)
            return {
                'analysis': response.text,
                'my_rank': my_rank,
                'competitor_count': len(competitors),
                'avg_metrics': {
                    'word_count': avg_word_count,
                    'h2_count': avg_h2_count,
                    'image_count': avg_image_count
                }
            }
        except Exception as e:
            return {
                'error': str(e)
            }
    
    def analyze_article_with_ai_competitive(self, keyword, url, content, metrics):
        """競合分析を含むAI記事分析（拡張版）"""
        # 基本的な記事分析
        basic_analysis = self.analyze_article_with_ai(keyword, url, content, metrics)
        
        # 競合分析を実行
        competitive_analysis = self.compare_with_competitors(keyword, url, content)
        
        if competitive_analysis and 'analysis' in competitive_analysis:
            combined_analysis = f"""
{basic_analysis}

============================================================
🔍 競合分析結果
============================================================

{competitive_analysis['analysis']}
            """
            return combined_analysis
        else:
            return basic_analysis
    
    def generate_overall_ai_analysis(self, trend_data, performance_data, conversion_data, intent_data):
        """全体的なAI分析を生成"""
        if not self.gemini_model:
            return None
        
        try:
            # トレンドサマリー
            trend_summary = ""
            if not trend_data.empty:
                top_trends = trend_data.head(10)
                trend_summary = f"急成長キーワード: {', '.join(top_trends['検索キーワード'].tolist())}"
            
            # パフォーマンスサマリー
            perf_summary = ""
            if not performance_data.empty:
                current = performance_data[performance_data['period'] == '現在期間'].iloc[0]
                comparison = performance_data[performance_data['period'] == '比較期間'].iloc[0]
                perf_summary = f"クリック数変化: {current['total_clicks']} (前期間: {comparison['total_clicks']})"
            
            # CTR改善機会サマリー
            ctr_summary = ""
            if not intent_data.empty:
                ctr_summary = f"CTR改善機会: {len(intent_data)}件のキーワード"
            
            prompt = f"""
            SEO専門家として、以下のデータに基づいて包括的な分析と改善提案を行ってください。

            【分析データサマリー】
            {trend_summary}
            {perf_summary}
            {ctr_summary}

            【分析してください】
            
            ■ トレンド要因分析
            - クリック数変化の主要要因
            - 外部環境の影響（アルゴリズム変更等）
            - 改善/悪化したキーワードの特徴

            ■ パフォーマンス課題
            - 全体的な検索パフォーマンスの評価
            - 主要な問題点の特定
            - 競合との比較観点

            ■ CV改善機会
            - 高CVRページの成功要因
            - 低CVRページの改善ポイント
            - コンバージョン最適化の具体策

            ■ CTR改善提案
            - 表示回数多い×CTR低いキーワードの対策
            - タイトル・メタディスクリプション改善案
            - リッチスニペット活用提案

            ■ 優先順位付け改善提案
            1. 最優先で実施すべき施策（3つ）
            2. 中期的に取り組むべき施策（3つ）
            3. 継続的にモニタリングすべき指標

            具体的で実行可能な提案をお願いします。
            """
            
            response = self.gemini_model.generate_content(prompt)
            return response.text
            
        except Exception as e:
            return f"AI分析エラー: {str(e)}"

def main():
    st.set_page_config(
        page_title="SEO分析ツール - Streamlit版",
        page_icon="🚀",
        layout="wide"
    )
    
    st.title("🚀 SEO分析ツール - Streamlit版")
    st.markdown("---")
    
    # 分析器初期化
    if 'analyzer' not in st.session_state:
        with st.spinner("初期化中..."):
            st.session_state.analyzer = SEOAnalyzerStreamlit()

        # セッション状態の初期化（この部分を追加）
    # セッション状態の初期化（この部分を追加）
    if 'article_analyses' not in st.session_state:
        st.session_state.article_analyses = []
    
    # 競合分析用の初期化も追加
    if 'daily_queries' not in st.session_state:
        st.session_state.daily_queries = 0
    if 'query_date' not in st.session_state:
        st.session_state.query_date = datetime.now().date()
    
    analyzer = st.session_state.analyzer

    
    # サイドバー
    with st.sidebar:
        st.header("📊 分析設定")
        
        # 闇金データベース固定
        selected_site = analyzer.config['sites'][0]
        st.info(f"分析対象: {selected_site['name']}")
        
        # 期間設定
        st.markdown("### 📅 分析期間")
        
        # 期間設定モード選択
        period_mode = st.radio(
            "期間設定モード",
            ["シンプル（前期間と自動比較）", "詳細設定（期間を個別指定）"],
            key="period_mode"
        )
        
        if period_mode == "シンプル（前期間と自動比較）":
            days_ago = st.slider("分析日数", 7, 90, 30, label_visibility="collapsed")
            st.caption(f"📊 現在: 直近{days_ago}日 vs 比較: {days_ago*2}〜{days_ago+1}日前")
            
            # 内部的な日付計算
            current_end = datetime.now().date()
            current_start = current_end - timedelta(days=days_ago)
            comparison_end = current_start - timedelta(days=1)
            comparison_start = comparison_end - timedelta(days=days_ago)
            
        else:  # 詳細設定モード
            st.markdown("**現在期間**")
            col1, col2 = st.columns(2)
            with col1:
                current_start = st.date_input(
                    "開始日",
                    value=datetime.now().date() - timedelta(days=30),
                    max_value=datetime.now().date(),
                    key="current_start"
                )
            with col2:
                current_end = st.date_input(
                    "終了日",
                    value=datetime.now().date(),
                    max_value=datetime.now().date(),
                    key="current_end"
                )
            
            st.markdown("**比較期間**")
            
            # 比較期間のプリセット
            comparison_preset = st.selectbox(
                "プリセット選択",
                ["前期間（自動）", "1ヶ月前", "3ヶ月前", "6ヶ月前", "1年前", "カスタム"],
                key="comparison_preset"
            )
            
            if comparison_preset == "前期間（自動）":
                period_days = (current_end - current_start).days
                comparison_end = current_start - timedelta(days=1)
                comparison_start = comparison_end - timedelta(days=period_days)
            elif comparison_preset == "1ヶ月前":
                comparison_start = current_start - timedelta(days=30)
                comparison_end = current_end - timedelta(days=30)
            elif comparison_preset == "3ヶ月前":
                comparison_start = current_start - timedelta(days=90)
                comparison_end = current_end - timedelta(days=90)
            elif comparison_preset == "6ヶ月前":
                comparison_start = current_start - timedelta(days=180)
                comparison_end = current_end - timedelta(days=180)
            elif comparison_preset == "1年前":
                comparison_start = current_start - timedelta(days=365)
                comparison_end = current_end - timedelta(days=365)
            else:  # カスタム
                col1, col2 = st.columns(2)
                with col1:
                    comparison_start = st.date_input(
                        "開始日",
                        value=current_start - timedelta(days=60),
                        max_value=current_start - timedelta(days=1),
                        key="comparison_start"
                    )
                with col2:
                    comparison_end = st.date_input(
                        "終了日",
                        value=current_end - timedelta(days=60),
                        max_value=current_start - timedelta(days=1),
                        key="comparison_end"
                    )
            
            # 期間サマリー表示
            st.info(f"""
            📊 期間設定
            - 現在: {current_start.strftime('%Y/%m/%d')} 〜 {current_end.strftime('%Y/%m/%d')} ({(current_end - current_start).days + 1}日間)
            - 比較: {comparison_start.strftime('%Y/%m/%d')} 〜 {comparison_end.strftime('%Y/%m/%d')} ({(comparison_end - comparison_start).days + 1}日間)
            """)
            
            # 期間の妥当性チェック
            if comparison_end >= current_start:
                st.error("⚠️ 比較期間と現在期間が重複しています")
            
            days_ago = (current_end - current_start).days
        
        # 詳細設定
        with st.expander("詳細設定"):
            change_threshold = st.slider("変化率閾値(%)", 10, 200, 10)
            min_clicks = st.number_input("最小クリック数", 1, 50, 1)
            ctr_threshold = st.slider("CTR閾値(%)", 1.0, 20.0, 5.0) / 100
            min_impressions = st.number_input("最小表示回数", 50, 1000, 100)
        
        # 実行ボタン
        if st.button("📊 分析実行", type="primary", use_container_width=True):
            st.session_state.run_analysis = True
            st.session_state.site = selected_site
            st.session_state.analysis_current_start = current_start
            st.session_state.analysis_current_end = current_end
            st.session_state.analysis_comparison_start = comparison_start
            st.session_state.analysis_comparison_end = comparison_end
            st.session_state.days_ago = days_ago  # これを追加
            st.session_state.change_threshold = change_threshold
            st.session_state.min_clicks = min_clicks
            st.session_state.ctr_threshold = ctr_threshold
            st.session_state.min_impressions = min_impressions
            # 分析完了フラグをリセット
            if 'analysis_completed' in st.session_state:
                del st.session_state.analysis_completed



    
    # メイン画面
    if st.session_state.get('run_analysis'):
        site = st.session_state.site
        
        # cache_keyを最初に定義
        cache_key = f"{site['name']}_{st.session_state.analysis_current_start}_{st.session_state.analysis_current_end}"


        
        if 'data_cache' not in st.session_state:
            st.session_state.data_cache = {}
        
        if cache_key not in st.session_state.data_cache:
            # 初回のみデータ取得
            # with st.spinner("データ取得中..."):  # コメントアウト
            current_gsc, comparison_gsc = analyzer.get_gsc_data(
                site['gsc_url'], 
                st.session_state.analysis_current_start,
                st.session_state.analysis_current_end,
                st.session_state.analysis_comparison_start,
                st.session_state.analysis_comparison_end
            )

            
            if current_gsc is None:
                st.error("GSCデータの取得に失敗しました")
                st.stop()
            
            ga4_data = analyzer.get_ga4_data(
                site['ga4_property_id'],
                st.session_state.analysis_current_start,
                st.session_state.analysis_current_end
            )
            
            # キャッシュに保存
            st.session_state.data_cache[cache_key] = {
                'current_gsc': current_gsc,
                'comparison_gsc': comparison_gsc,
                'ga4_data': ga4_data
            }

        
        # キャッシュからデータ取得
        cached_data = st.session_state.data_cache[cache_key]
        current_gsc = cached_data['current_gsc']
        comparison_gsc = cached_data['comparison_gsc']
        ga4_data = cached_data['ga4_data']
        
        # 分析実行（キャッシュ利用）
        if 'analysis_results_cache' not in st.session_state:
            st.session_state.analysis_results_cache = {}
        
        if cache_key not in st.session_state.analysis_results_cache:
            # with st.spinner("分析中..."):  # コメントアウト
            trend_data = analyzer.analyze_trends(
                current_gsc, comparison_gsc,
                st.session_state.change_threshold,
                st.session_state.min_clicks
            )
            
            performance_data = analyzer.analyze_performance(
                current_gsc, comparison_gsc
            )
            
            conversion_data = analyzer.analyze_conversion(ga4_data)
            
            intent_data = analyzer.analyze_search_intent(
                current_gsc,
                st.session_state.ctr_threshold,
                st.session_state.min_impressions
            )
            
            # 全体的なAI分析
            overall_analysis = analyzer.generate_overall_ai_analysis(
                trend_data, performance_data, conversion_data, intent_data
            )
            
            # キャッシュに保存
            st.session_state.analysis_results_cache[cache_key] = {
                'trend_data': trend_data,
                'performance_data': performance_data,
                'conversion_data': conversion_data,
                'intent_data': intent_data,
                'overall_analysis': overall_analysis
            }

        
        # キャッシュから分析結果取得
        cached_results = st.session_state.analysis_results_cache[cache_key]
        trend_data = cached_results['trend_data']
        performance_data = cached_results['performance_data']
        conversion_data = cached_results['conversion_data']
        intent_data = cached_results['intent_data']
        overall_analysis = cached_results['overall_analysis']
        
        # タブ選択を保持（この部分を追加）
        if 'preserve_tab' not in st.session_state:
            st.session_state.preserve_tab = 0

        
        # 分析実行完了時にトレンド分析タブに移動
        if 'analysis_completed' not in st.session_state:
            st.session_state.active_tab = 1  # トレンド分析タブ
            st.session_state.analysis_completed = True
        
        
        # 結果表示（以下既存のコード）
        tabs = st.tabs([
            "📊 ダッシュボード",
            "📈 トレンド分析",
            "📊 パフォーマンス比較", 
            "💰 コンバージョン分析",
            "🎯 検索意図分析",
            "📝 記事詳細分析",
            "📚 分析履歴",
            "💬 AIアシスタント"
        ])

        # タブ状態をセッションで管理
        if 'current_tab' not in st.session_state:
            st.session_state.current_tab = 0

        
        with tabs[0]:  # ダッシュボード
            st.subheader("🚀 SEO分析ダッシュボード")
            
            # サマリーメトリクス
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                current_clicks = current_gsc['clicks'].sum()
                comparison_clicks = comparison_gsc['clicks'].sum()
                change_rate = ((current_clicks - comparison_clicks) / comparison_clicks * 100) if comparison_clicks > 0 else 0
                st.metric(
                    "総クリック数",
                    f"{current_clicks:,}",
                    f"{change_rate:+.1f}%"
                )
            
            with col2:
                current_imp = current_gsc['impressions'].sum()
                comparison_imp = comparison_gsc['impressions'].sum()
                change_rate = ((current_imp - comparison_imp) / comparison_imp * 100) if comparison_imp > 0 else 0
                st.metric(
                    "総表示回数",
                    f"{current_imp:,}",
                    f"{change_rate:+.1f}%"
                )
            
            with col3:
                current_ctr = current_gsc['ctr'].mean() * 100
                comparison_ctr = comparison_gsc['ctr'].mean() * 100
                st.metric(
                    "平均CTR",
                    f"{current_ctr:.2f}%",
                    f"{current_ctr - comparison_ctr:+.2f}%"
                )
            
            with col4:
                current_pos = current_gsc['position'].mean()
                comparison_pos = comparison_gsc['position'].mean()
                st.metric(
                    "平均順位",
                    f"{current_pos:.1f}",
                    f"{comparison_pos - current_pos:+.1f}"  # 順位は低い方が良い
                )



            
            st.markdown("---")
            
            # AI分析結果
            if overall_analysis:
                st.subheader("🤖 AI分析結果・改善提案")
                st.markdown(overall_analysis)
            
            st.markdown("---")
            
            # 重要な改善機会
            st.subheader("🎯 重要な改善機会")
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.markdown("**急成長キーワード**")
                if not trend_data.empty:
                    # 文字列と数値が混在している場合の処理
                    top_growth = trend_data[
                        (trend_data['変化率(%)'] == "新規") | 
                        (pd.to_numeric(trend_data['変化率(%)'], errors='coerce').fillna(0) > 0)
                    ].head(5)
                    
                    for _, row in top_growth.iterrows():
                        if row['変化率(%)'] == "新規":
                            st.write(f"• {row['検索キーワード']} (🆕 新規)")
                        else:
                            st.write(f"• {row['検索キーワード']} (+{row['変化率(%)']:.0f}%)")
                else:
                    st.info("該当なし")
            
            with col2:
                st.markdown("**CTR改善機会**")
                if not intent_data.empty:
                    for _, row in intent_data.head(5).iterrows():
                        st.write(f"• {row['検索キーワード']} (CTR: {row['CTR']:.1f}%)")
                else:
                    st.info("該当なし")
            
            with col3:
                st.markdown("**高CVRページ**")
                if not conversion_data.empty and conversion_data['CVR(%)'].max() > 0:
                    top_cvr = conversion_data.head(5)
                    for _, row in top_cvr.iterrows():
                        st.write(f"• {row['ページパス']} (CVR: {row['CVR(%)']:.1f}%)")
                else:
                    st.info("該当なし")
        
        with tabs[1]:  # トレンド分析
            st.subheader("📈 トレンド分析 - クリック数が大幅に変化したキーワード")
            
            if not trend_data.empty:
                # メトリクス表示
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("分析キーワード数", len(trend_data))
                with col2:
                    # 数値のみで平均を計算
                    numeric_rates = pd.to_numeric(trend_data['変化率(%)'], errors='coerce')
                    avg_change = numeric_rates.dropna().mean()
                    st.metric("平均変化率", f"{avg_change:.1f}%")

                with col3:
                    positive = len(trend_data[trend_data['クリック数変化'] > 0])
                    st.metric("改善キーワード", positive)

            # AI分析結果
            if overall_analysis:
                st.subheader("🤖 AI分析結果・改善提案")
                st.markdown(overall_analysis)
                # ここに追加
                st.success("✅ 分析完了！")
                st.warning("📊 **トレンド分析**タブまたは**検索意図分析**タブをクリックして詳細結果をご確認ください")

                
                st.markdown("---")
                
                # 検索とソート機能
                col1, col2, col3 = st.columns([2, 2, 1])
                
                with col1:
                    # キーワード検索
                    search_keyword = st.text_input(
                        "🔍 キーワード検索",
                        placeholder="検索したいキーワードを入力",
                        key="trend_search"
                    )
                
                with col2:
                    # ソート選択（トレンド分析用）
                    sort_options = {
                        "クリック数変化（降順）": ("クリック数変化", False),
                        "クリック数変化（昇順）": ("クリック数変化", True),
                        "変化率（降順）": ("変化率(%)", False),
                        "変化率（昇順）": ("変化率(%)", True),
                        "現在クリック数（降順）": ("現在期間_クリック数", False),
                        "現在クリック数（昇順）": ("現在期間_クリック数", True),
                        "現在表示回数（降順）": ("現在期間_表示回数", False),
                        "現在表示回数（昇順）": ("現在期間_表示回数", True),
                        "CTR（降順）": ("現在期間_CTR", False),
                        "CTR（昇順）": ("現在期間_CTR", True),
                        "平均順位（上位順）": ("現在期間_平均順位", True),
                        "平均順位（下位順）": ("現在期間_平均順位", False)
                    }
                    
                    # セッション状態の値が無効な場合はデフォルトに戻す
                    if st.session_state.get('trend_sort') not in sort_options:
                        st.session_state.trend_sort = "クリック数変化（降順）"
                    
                    selected_sort = st.selectbox(
                        "📊 並び替え",
                        options=list(sort_options.keys()),
                        key="trend_sort"
                    )
                    
                    sort_column, ascending = sort_options[selected_sort]

                
                with col3:
                    # 表示件数
                    if len(intent_data) > 0:  # 追加
                        display_count = st.number_input(
                            "表示件数",
                            min_value=1,
                            max_value=max(1, len(intent_data)),
                            value=min(50, len(intent_data)),
                            step=10,
                            key="intent_display"
                        )
                    else:  # 追加
                        display_count = 0  # 追加



                
                # フィルタリングとソート
                filtered_data = trend_data.copy()
                
                # キーワード検索フィルタ
                if search_keyword:
                    filtered_data = filtered_data[
                        filtered_data['検索キーワード'].str.contains(
                            search_keyword, case=False, na=False
                        )
                    ]
                
                # ソート実行
                # 列名を強制的に修正
                if sort_column == "表示回数":
                    sort_column = "現在期間_表示回数"
                elif sort_column == "クリック数":
                    sort_column = "現在期間_クリック数"
                
                # 変化率でソートする場合の特別処理
                if sort_column == "変化率(%)":
                    # 新規を一時的に数値に変換してソート
                    temp_df = filtered_data.copy()
                    temp_df['sort_key'] = temp_df['変化率(%)'].apply(
                        lambda x: 999999 if x == "新規" else x
                    )
                    filtered_data = temp_df.sort_values(
                        by='sort_key',
                        ascending=ascending
                    ).drop('sort_key', axis=1)
                else:
                    filtered_data = filtered_data.sort_values(
                        by=sort_column,
                        ascending=ascending
                    )


                
                # 検索結果の件数表示
                if search_keyword:
                    st.info(f"検索結果: {len(filtered_data)}件")
                
                st.markdown("---")
                
                # ヘッダー行（ボタンなしバージョン）
                col1, col2, col3, col4, col5, col6, col7, col8 = st.columns([2.5, 0.8, 1, 1, 1, 1, 1, 1])
                with col1:
                    st.markdown("**キーワード / URL**")
                with col2:
                    st.markdown("**分析**")
                with col3:
                    st.markdown("**現在**")
                with col4:
                    st.markdown("**前期間**")
                with col5:
                    st.markdown("**変化**")
                with col6:
                    st.markdown("**変化率**")
                with col7:
                    st.markdown("**CTR**")
                with col8:
                    st.markdown("**順位**")
                
                # データを行ごとに表示（分析ボタン付き）
                for idx, row in filtered_data.iterrows():
                    col1, col2, col3, col4, col5, col6, col7, col8 = st.columns([2.5, 0.8, 1, 1, 1, 1, 1, 1])
                    
                    with col1:
                        st.write(f"**{row['検索キーワード']}**")
                        st.caption(f"{row['ページURL']}")
                    
                    with col2:
                        with st.popover("🔍"):
                            analysis_mode = st.radio(
                                "分析モード",
                                ["基本分析", "競合分析込み"],
                                key=f"mode_trend_{idx}",
                                help="競合分析は1回1クエリ消費します"
                            )
                            
                            if st.button("分析実行", key=f"exec_trend_{idx}"):
                                with st.spinner("分析中..."):
                                    content = analyzer.fetch_article_content(
                                        row['ページURL'],
                                        site['gsc_url']
                                    )
                                    
                                    if content['success']:
                                        # 分析モードに応じて処理
                                        if analysis_mode == "競合分析込み":
                                            # 使用量確認
                                            if st.session_state.daily_queries >= 100 and not st.session_state.get('payment_approved', False):
                                                st.error("無料枠を超過しています")
                                            else:
                                                analysis = analyzer.analyze_article_with_ai_competitive(
                                                    row['検索キーワード'],
                                                    row['ページURL'],
                                                    content,
                                                    {
                                                        'clicks': row['現在期間_クリック数'],
                                                        'impressions': row['現在期間_表示回数'],
                                                        'ctr': row['現在期間_CTR'] * 100,
                                                        'position': row['現在期間_平均順位']
                                                    }
                                                )
                                        else:
                                            analysis = analyzer.analyze_article_with_ai(
                                                row['検索キーワード'],
                                                row['ページURL'],
                                                content,
                                                {
                                                    'clicks': row['現在期間_クリック数'],
                                                    'impressions': row['現在期間_表示回数'],
                                                    'ctr': row['現在期間_CTR'] * 100,
                                                    'position': row['現在期間_平均順位']
                                                }
                                            )
                                        
                                        st.session_state.article_analyses.append({
                                            'keyword': row['検索キーワード'],
                                            'url': row['ページURL'],
                                            'analysis': analysis,
                                            'metrics': row.to_dict(),
                                            'mode': analysis_mode
                                        })
                                        
                                        # 分析結果を自動保存（この部分を追加）
                                        saved_file = analyzer.save_analysis_result(
                                            row['検索キーワード'],
                                            row['ページURL'],
                                            analysis,
                                            analysis_mode
                                        )
                                        
                                        st.success("✅ 分析完了！")
                                        st.info(f"📁 保存済み: {saved_file}")
                                        # st.balloons()



                                    else:
                                        st.error(f"記事取得エラー: {content['error']}")
                    
                    with col3:
                        st.metric("", f"{row['現在期間_クリック数']:,.0f}")
                    
                    with col4:
                        st.metric("", f"{row['比較期間_クリック数']:,.0f}")
                    
                    with col5:
                        color = "🟢" if row['クリック数変化'] > 0 else "🔴"
                        st.write(f"{color} {row['クリック数変化']:+,.0f}")
                    
                    with col6:
                        if row['変化率(%)'] == "新規":
                            st.write("🆕 新規")
                        else:
                            color = "🟢" if row['変化率(%)'] > 0 else "🔴"
                            st.write(f"{color} {row['変化率(%)']:+.1f}%")
                    
                    with col7:
                        st.write(f"{row['現在期間_CTR']*100:.2f}%")
                    
                    with col8:
                        st.write(f"{row['現在期間_平均順位']:.1f}")
                    
                    st.divider()
                
                # ページネーション的な表示
                if len(trend_data) > display_count:
                    st.caption(f"全{len(trend_data)}件中 {display_count}件を表示")
                
                # 凡例
                st.caption("🔍 = 記事分析 | 🟢 = 改善 | 🔴 = 悪化 | ↕ = クリックで並び替え")
            else:
                st.info("該当するキーワードがありません")


        
        with tabs[2]:  # パフォーマンス比較
            st.subheader("📊 パフォーマンス比較")
            
            if not performance_data.empty:
                # 比較表
                st.dataframe(
                    performance_data.style.format({
                        'total_clicks': '{:,.0f}',
                        'total_impressions': '{:,.0f}',
                        'avg_ctr': '{:.2f}%',
                        'avg_position': '{:.1f}'
                    }),
                    use_container_width=True
                )
                
                st.markdown("---")
                
                # グラフ表示
                col1, col2 = st.columns(2)
                
                with col1:
                    fig = px.bar(
                        performance_data,
                        x='period',
                        y='total_clicks',
                        title='クリック数比較',
                        color='period',
                        color_discrete_map={'現在期間': '#1f77b4', '比較期間': '#ff7f0e'}
                    )
                    st.plotly_chart(fig, use_container_width=True, key="perf_clicks_chart")
                
                with col2:
                    fig = px.bar(
                        performance_data,
                        x='period',
                        y='avg_ctr',
                        title='平均CTR比較',
                        color='period',
                        color_discrete_map={'現在期間': '#1f77b4', '比較期間': '#ff7f0e'}
                    )
                    st.plotly_chart(fig, use_container_width=True, key="perf_ctr_chart")
                
                # 詳細分析
                st.markdown("---")
                st.subheader("📊 詳細分析")
                
                current = performance_data[performance_data['period'] == '現在期間'].iloc[0]
                comparison = performance_data[performance_data['period'] == '比較期間'].iloc[0]
                
                col1, col2 = st.columns(2)
                
                with col1:
                    st.markdown("**変化率**")
                    click_change = ((current['total_clicks'] - comparison['total_clicks']) / comparison['total_clicks'] * 100) if comparison['total_clicks'] > 0 else 0
                    imp_change = ((current['total_impressions'] - comparison['total_impressions']) / comparison['total_impressions'] * 100) if comparison['total_impressions'] > 0 else 0
                    
                    st.write(f"• クリック数: {click_change:+.1f}%")
                    st.write(f"• 表示回数: {imp_change:+.1f}%")
                    st.write(f"• CTR: {current['avg_ctr'] - comparison['avg_ctr']:+.2f}ポイント")
                    st.write(f"• 平均順位: {comparison['avg_position'] - current['avg_position']:+.1f}")
                
                with col2:
                    st.markdown("**キーワード数**")
                    st.write(f"• 現在: {current['unique_queries']:,}個")
                    st.write(f"• 比較期間: {comparison['unique_queries']:,}個")
                    st.write(f"• 増減: {current['unique_queries'] - comparison['unique_queries']:+,}個")
        
        with tabs[3]:  # コンバージョン分析
            st.subheader("💰 コンバージョン分析")
            
            if not conversion_data.empty:
                # CVR上位ページ
                st.dataframe(
                    conversion_data.head(20).style.format({
                        'セッション数': '{:,.0f}',
                        'ユーザー数': '{:,.0f}',
                        '直帰率': '{:.1f}%',
                        '平均セッション時間': '{:.1f}',
                        'コンバージョン数': '{:,.0f}',
                        'CVR(%)': '{:.2f}%'
                    }),
                    use_container_width=True,
                    height=400
                )
                
                # CVRグラフ
                if conversion_data['CVR(%)'].max() > 0:
                    st.markdown("---")
                    fig = px.bar(
                        conversion_data.head(10),
                        x='ページパス',
                        y='CVR(%)',
                        title='CVR上位10ページ',
                        color='CVR(%)',
                        color_continuous_scale='Blues'
                    )
                    fig.update_layout(xaxis_tickangle=-45)
                    st.plotly_chart(fig, use_container_width=True, key="cvr_chart")
            else:
                st.info("コンバージョンデータがありません")
        
        with tabs[4]:  # 検索意図分析
            st.subheader("🎯 検索意図分析 - 改善機会のあるキーワード")
            
            if not intent_data.empty:
                # 改善機会の概要
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("対象キーワード数", len(intent_data))
                with col2:
                    total_impressions = intent_data['表示回数'].sum()
                    st.metric("合計表示回数", f"{total_impressions:,}")
                with col3:
                    avg_ctr = intent_data['CTR'].mean()
                    st.metric("平均CTR", f"{avg_ctr:.2f}%")
                
                st.markdown("---")
                
                # 検索とソート機能
                col1, col2, col3 = st.columns([2, 2, 1])
                
                with col1:
                    # キーワード検索
                    search_keyword = st.text_input(
                        "🔍 キーワード検索",
                        placeholder="検索したいキーワードを入力",
                        key="intent_search"
                    )
                
                with col2:
                    # ソート選択
                    sort_options = {
                        "表示回数（降順）": ("表示回数", False),
                        "表示回数（昇順）": ("表示回数", True),
                        "CTR（昇順）": ("CTR", True),  # CTRは低い順が重要
                        "CTR（降順）": ("CTR", False),
                        "クリック数（降順）": ("クリック数", False),
                        "クリック数（昇順）": ("クリック数", True),
                        "平均順位（上位順）": ("平均掲載順位", True),
                        "平均順位（下位順）": ("平均掲載順位", False)
                    }
                    
                    selected_sort = st.selectbox(
                        "📊 並び替え",
                        options=list(sort_options.keys()),
                        key="intent_sort"
                    )
                    
                    sort_column, ascending = sort_options[selected_sort]
                
                with col3:
                    # 表示件数
                    display_count = st.number_input(
                        "表示件数",
                        min_value=1,
                        max_value=max(1, len(intent_data)),
                        value=min(50, len(intent_data)),
                        step=10,
                        key="intent_display"
                    )

                
                # フィルタリングとソート
                filtered_data = intent_data.copy()
                
                # キーワード検索フィルタ
                if search_keyword:
                    filtered_data = filtered_data[
                        filtered_data['検索キーワード'].str.contains(
                            search_keyword, case=False, na=False
                        )
                    ]
                
                # ソート実行
                filtered_data = filtered_data.sort_values(
                    by=sort_column,
                    ascending=ascending
                )
                
                # 検索結果の件数表示（head適用前に表示）
                if search_keyword:
                    st.info(f"検索結果: {len(filtered_data)}件")
                
                # 表示件数制限を最後に適用
                filtered_data = filtered_data.head(display_count)

                
                st.markdown("---")
                
                # ヘッダー行
                col1, col2, col3, col4, col5, col6 = st.columns([2.5, 0.8, 1.2, 1, 1, 1])
                with col1:
                    st.markdown("**キーワード / URL**")
                with col2:
                    st.markdown("**分析**")
                with col3:
                    st.markdown("**表示**")
                with col4:
                    st.markdown("**クリック**")
                with col5:
                    st.markdown("**CTR**")
                with col6:
                    st.markdown("**順位**")
                
                st.divider()
                
                # データを行ごとに表示（分析ボタン付き）
                for idx, row in filtered_data.iterrows():
                    col1, col2, col3, col4, col5, col6 = st.columns([2.5, 0.8, 1.2, 1, 1, 1])
                    
                    with col1:
                        st.write(f"**{row['検索キーワード']}**")
                        st.caption(f"{row['ページURL']}")
                    
                    with col2:
                        with st.popover("🔍"):
                            analysis_mode = st.radio(
                                "分析モード",
                                ["基本分析", "競合分析込み"],
                                key=f"mode_intent_{idx}",
                                help="競合分析は1回1クエリ消費します"
                            )
                            
                            if st.button("分析実行", key=f"exec_intent_{idx}"):
                                with st.spinner("分析中..."):
                                    content = analyzer.fetch_article_content(
                                        row['ページURL'],
                                        site['gsc_url']
                                    )
                                    
                                    if content['success']:
                                        # 分析モードに応じて処理
                                        if analysis_mode == "競合分析込み":
                                            # 使用量確認
                                            if st.session_state.daily_queries >= 100 and not st.session_state.get('payment_approved', False):
                                                st.error("無料枠を超過しています")
                                            else:
                                                analysis = analyzer.analyze_article_with_ai_competitive(
                                                    row['検索キーワード'],
                                                    row['ページURL'],
                                                    content,
                                                    {
                                                        'clicks': row['クリック数'],
                                                        'impressions': row['表示回数'],
                                                        'ctr': row['CTR'],
                                                        'position': row['平均掲載順位']
                                                    }
                                                )
                                        else:
                                            analysis = analyzer.analyze_article_with_ai(
                                                row['検索キーワード'],
                                                row['ページURL'],
                                                content,
                                                {
                                                    'clicks': row['クリック数'],
                                                    'impressions': row['表示回数'],
                                                    'ctr': row['CTR'],
                                                    'position': row['平均掲載順位']
                                                }
                                            )
                                        
                                        if 'article_analyses' not in st.session_state:
                                            st.session_state.article_analyses = []
                                        
                                        st.session_state.article_analyses.append({
                                            'keyword': row['検索キーワード'],
                                            'url': row['ページURL'],
                                            'analysis': analysis,
                                            'metrics': row.to_dict(),
                                            'mode': analysis_mode
                                        })
                                        
                                        # 分析結果を自動保存
                                        saved_file = analyzer.save_analysis_result(
                                            row['検索キーワード'],
                                            row['ページURL'],
                                            analysis,
                                            analysis_mode
                                        )
                                        
                                        st.success("✅ 分析完了！")
                                        st.info(f"📁 保存済み: {saved_file}")
                                        # st.balloons()
                                    else:
                                        st.error(f"記事取得エラー: {content['error']}")
                    
                    with col3:
                        st.metric("", f"{row['表示回数']:,}")
                    
                    with col4:
                        st.metric("", f"{row['クリック数']:,}")
                    
                    with col5:
                        # CTRが低いほど赤く表示
                        ctr_color = "🔴" if row['CTR'] < 3 else "🟡" if row['CTR'] < 5 else "🟢"
                        st.write(f"{ctr_color} {row['CTR']:.2f}%")
                    
                    with col6:
                        st.write(f"#{row['平均掲載順位']:.1f}")
                    
                    st.divider()
                
                # ページネーション的な表示
                if len(intent_data) > display_count:
                    st.caption(f"全{len(intent_data)}件中 {display_count}件を表示")
                
                # 凡例
                st.caption("🔍 = 記事分析 | CTR: 🔴 < 3% | 🟡 3-5% | 🟢 > 5%")
            else:
                st.info("改善機会のあるキーワードがありません")
        
        with tabs[5]:  # 記事詳細分析
            st.subheader("📝 記事詳細分析結果")
            
            if 'article_analyses' in st.session_state and st.session_state.article_analyses:
                for i, analysis in enumerate(st.session_state.article_analyses):
                    with st.expander(f"🔍 {analysis['keyword']} ({analysis['mode']})", expanded=True):
                        col1, col2 = st.columns([3, 1])
                        
                        with col1:
                            st.write(f"**URL:** {analysis['url']}")
                        
                        with col2:
                            if st.button("削除", key=f"del_{i}"):
                                st.session_state.article_analyses.pop(i)
                                st.rerun()
                        
                        st.markdown("---")
                        st.markdown("**📊 パフォーマンス指標:**")
                        
                        metrics = analysis['metrics']
                        col1, col2, col3, col4 = st.columns(4)
                        
                        # メトリクスの表示（キーの存在確認）
                        with col1:
                            clicks = metrics.get('現在期間_クリック数', metrics.get('クリック数', 'N/A'))
                            st.metric("クリック数", clicks)
                        with col2:
                            impressions = metrics.get('現在期間_表示回数', metrics.get('表示回数', 'N/A'))
                            st.metric("表示回数", impressions)
                        with col3:
                            ctr = metrics.get('現在期間_CTR', metrics.get('CTR', 0))
                            if isinstance(ctr, (int, float)):
                                ctr_display = f"{ctr * 100:.2f}%" if ctr < 1 else f"{ctr:.2f}%"
                            else:
                                ctr_display = "N/A"
                            st.metric("CTR", ctr_display)
                        with col4:
                            position = metrics.get('現在期間_平均順位', metrics.get('平均掲載順位', 'N/A'))
                            if isinstance(position, (int, float)):
                                position_display = f"{position:.1f}"
                            else:
                                position_display = "N/A"
                            st.metric("平均順位", position_display)
                        
                        st.markdown("---")
                        st.markdown("**🤖 AI分析結果:**")
                        st.write(analysis['analysis'])
                        
                        st.markdown("---")
                
                # エクスポートボタン
                col1, col2 = st.columns([1, 3])
                with col1:
                    if st.button("📥 分析結果をダウンロード", type="secondary"):
                        # 分析結果をテキストファイルとして出力
                        export_text = f"SEO記事分析レポート\n"
                        export_text += f"生成日時: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
                        export_text += f"サイト: {site['name']}\n"
                        export_text += "=" * 80 + "\n\n"
                        
                        for analysis in st.session_state.article_analyses:
                            export_text += f"キーワード: {analysis['keyword']}\n"
                            export_text += f"URL: {analysis['url']}\n"
                            export_text += f"分析モード: {analysis['mode']}\n"
                            export_text += f"\n分析結果:\n{analysis['analysis']}\n"
                            export_text += "=" * 80 + "\n\n"
                        
                        st.download_button(
                            label="📄 テキストファイルをダウンロード",
                            data=export_text,
                            file_name=f"seo_analysis_{site['name']}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt",
                            mime="text/plain"
                        )
                
                with col2:
                    if st.button("🗑️ 全ての分析結果をクリア"):
                        st.session_state.article_analyses = []
                        st.rerun()
            else:
                st.info("まだ記事分析を実行していません。他のタブから記事を選択して「🔍」ボタンをクリックしてください。")

        with tabs[6]:
            st.header("📚 分析履歴")
            
            # サイトフィルタ
            col1, col2 = st.columns([3, 1])
            with col1:
                filter_site = st.selectbox(
                    "サイトでフィルタ",
                    ["すべて"] + [s['name'] for s in analyzer.config.get('sites', [])],
                    key="history_site_filter",
                    on_change=None  # on_changeを削除
                )

            with col2:
                if st.button("🔄 更新", key="refresh_history"):
                    st.rerun()
            
            # 履歴読み込み
            if filter_site == "すべて":
                history = analyzer.load_analysis_history()
            else:
                history = analyzer.load_analysis_history(site_name=filter_site)
            
            if history:
                for item in history:
                    # サイト名を安全に取得（古いファイル対応）
                    site_name = item.get('site', '不明')
                    
                    with st.expander(f"🕐 {item['timestamp']} - [{site_name}] {item['keyword']} ({item['mode']}) by {item['user']}"):
                        st.write(f"**サイト:** {site_name}")
                        st.write(f"**URL:** {item['url']}")
                        st.write("**分析結果:**")
                        st.write(item['analysis'])
            else:
                st.info("まだ分析履歴がありません")

        with tabs[7]:  # AIチャット
            st.header("💬 AIアシスタント")
            st.caption("分析結果について質問したり、SEOの相談ができます")
            
            # チャット履歴の初期化
            if 'chat_messages' not in st.session_state:
                st.session_state.chat_messages = []
            
            # チャット履歴を表示
            for message in st.session_state.chat_messages:
                with st.chat_message(message["role"]):
                    st.write(message["content"])
            
            # 入力欄
            if prompt := st.chat_input("質問を入力してください（例: トレンド分析の結果について詳しく教えて）"):
                # ユーザーメッセージを追加・表示
                st.session_state.chat_messages.append({"role": "user", "content": prompt})
                with st.chat_message("user"):
                    st.write(prompt)
                
                # AI応答生成
                with st.chat_message("assistant"):
                    with st.spinner("考え中..."):
                        # 現在の分析データからコンテキストを構築
                        context_parts = []
                        
                        # 基本情報
                        if 'site' in st.session_state:
                            context_parts.append(f"分析サイト: {st.session_state.site['name']}")
                        if 'days_ago' in st.session_state:
                            context_parts.append(f"分析期間: 直近{st.session_state.days_ago}日間")
                        
                        # 分析結果の要約
                        if 'analysis_results_cache' in st.session_state and cache_key in st.session_state.analysis_results_cache:
                            cached = st.session_state.analysis_results_cache[cache_key]
                            if 'trend_data' in cached and not cached['trend_data'].empty:
                                context_parts.append(f"トレンド分析: {len(cached['trend_data'])}件の変化を検出")
                            if 'intent_data' in cached and not cached['intent_data'].empty:
                                context_parts.append(f"CTR改善機会: {len(cached['intent_data'])}件")
                        
                        # 記事分析履歴
                        if 'article_analyses' in st.session_state and st.session_state.article_analyses:
                            context_parts.append(f"分析済み記事: {len(st.session_state.article_analyses)}件")
                            # 最新の分析内容も含める
                            latest = st.session_state.article_analyses[-1]
                            context_parts.append(f"最新分析: {latest['keyword']}（{latest['mode']}）")
                        
                        # プロンプト構築
                        full_prompt = f"""
                        あなたはSEO専門家のアシスタントです。ユーザーの質問に対して、現在の分析結果を踏まえて具体的で実用的なアドバイスを提供してください。
                        
                        【現在の分析コンテキスト】
                        {chr(10).join(context_parts)}
                        
                        【ユーザーからの質問】
                        {prompt}
                        
                        【回答ガイドライン】
                        - 具体的で実行可能なアドバイスを提供
                        - 現在の分析データを参照して回答
                        - 専門用語は分かりやすく説明
                        - 必要に応じて追加の分析を提案
                        """
                        
                        try:
                            response = analyzer.gemini_model.generate_content(full_prompt)
                            ai_response = response.text
                            
                            # 応答を表示
                            st.write(ai_response)
                            
                            # 履歴に追加
                            st.session_state.chat_messages.append({
                                "role": "assistant",
                                "content": ai_response
                            })
                            
                        except Exception as e:
                            st.error(f"エラーが発生しました: {e}")
                            st.write("申し訳ございません。回答の生成中にエラーが発生しました。")
            
            # チャット履歴のクリアボタン
            col1, col2, col3 = st.columns([1, 1, 3])
            with col1:
                if st.button("💾 会話を保存", disabled=len(st.session_state.chat_messages) == 0):
                    # 会話履歴をテキストファイルとして保存
                    chat_text = f"AIアシスタント会話履歴\n"
                    chat_text += f"日時: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
                    chat_text += f"サイト: {st.session_state.get('site', {}).get('name', '不明')}\n"
                    chat_text += "=" * 50 + "\n\n"
                    
                    for msg in st.session_state.chat_messages:
                        role = "ユーザー" if msg["role"] == "user" else "AI"
                        chat_text += f"【{role}】\n{msg['content']}\n\n"
                    
                    st.download_button(
                        label="📄 ダウンロード",
                        data=chat_text,
                        file_name=f"chat_history_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt",
                        mime="text/plain"
                    )
            
            with col2:
                if st.button("🗑️ 会話をクリア", disabled=len(st.session_state.chat_messages) == 0):
                    st.session_state.chat_messages = []
                    st.rerun()
            
            # サンプル質問
            with st.expander("💡 質問例"):
                st.markdown("""
                - このサイトのSEOで最優先で改善すべき点は？
                - トレンド分析の結果をもっと詳しく説明して
                - CTRが低いキーワードの改善方法を教えて
                - 競合分析の結果から、どんな施策を打つべき？
                - 新規流入キーワードを増やすには？
                - 分析結果から3ヶ月の改善計画を立てて
                """)



    
    else:
        # 初期画面
        st.info("左サイドバーから設定を選択して「📊 分析実行」をクリックしてください")
        
        # 使い方
        with st.expander("📖 使い方"):
            st.markdown("""
            ### SEO分析ツールの使い方
            
            1. **サイト選択**: 左サイドバーから分析したいサイトを選択
            2. **期間設定**: 分析期間を設定（デフォルト30日）
            3. **分析実行**: 「📊 分析実行」ボタンをクリック
            4. **結果確認**: 各タブで分析結果を確認
            5. **記事分析**: 気になる記事の「🔍」ボタンで詳細分析
               - 基本分析: 記事内容のみ分析
               - 競合分析込み: 実際の検索結果と比較（1クエリ消費）
            
            ### 各分析の説明
            - **ダッシュボード**: 全体的なパフォーマンスとAI分析結果
            - **トレンド分析**: クリック数が大幅に変化したキーワードを特定
            - **パフォーマンス比較**: 期間比較での全体的な変化を確認
            - **コンバージョン分析**: CVRの高いページを特定
            - **検索意図分析**: 表示は多いがクリックされていないキーワードを発見
            - **記事詳細分析**: 個別記事のAI分析結果を確認
            """)
        
        # 機能比較
        with st.expander("📊 Aパターン（スプレッドシート版）との比較"):
            st.markdown("""
            ### 機能比較
            
            | 機能 | Aパターン（スプシ版） | Bパターン（Streamlit版） |
            |------|----------------------|------------------------|
            | 4つの基本分析 | ✅ | ✅ |
            | AI全体分析 | ✅ | ✅ |
            | 個別記事分析 | ❌ | ✅ |
            | 競合分析 | ❌ | ✅ |
            | データ出力 | スプレッドシート | 画面表示・テキスト |
            | インタラクティブ | ❌ | ✅ |
            | 配布の容易さ | ✅（共有リンク） | △（環境構築必要） |
            
            ### それぞれの利点
            - **Aパターン**: 簡単共有、エクセル感覚で使える
            - **Bパターン**: リアルタイム分析、詳細な記事改善提案、競合比較
            """)

if __name__ == "__main__":
    main()


