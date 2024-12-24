import os
import sys
import sqlite3
from datetime import datetime
from pathlib import Path
from smb.SMBConnection import SMBConnection

# 写真ライブラリのパスをグローバル変数として定義
PHOTOS_LIBRARY = 'ここに.photoslibraryファイルのパスを入れる'

# NAS接続設定
NAS_HOST = 'NASのホスト名を入れる'  # .local を追加しmDNSで解決
NAS_IP = '名前解決できなかったとき用のIPアドレスを入れる'  # IPアドレスを文字列として指定
NAS_SHARE = '共有フォルダ名を入れる'
NAS_PATH = 'その後のパスを入れる'
NAS_USERNAME = 'ユーザー名'  # SMB接続用のユーザー名
NAS_PASSWORD = 'パスワード'  # SMB接続用のパスワード

def connect_to_photos_library():
    """写真ライブラリのデータベースに接続"""
    photos_db = os.path.join(PHOTOS_LIBRARY, 'database/Photos.sqlite')

    try:
        return sqlite3.connect(f"file:{photos_db}?mode=ro", uri=True)
    except sqlite3.Error as e:
        raise Exception(f"Failed to connect to database: {str(e)}")

def get_photos_info(conn):
    """写真の情報を取得"""
    try:
        cursor = conn.cursor()

        query = """
        SELECT
            ZASSET.ZUUID,
            ZASSET.ZFILENAME,
            strftime('%Y', datetime(ZASSET.ZDATECREATED + 978307200, 'unixepoch')) as year,
            strftime('%m', datetime(ZASSET.ZDATECREATED + 978307200, 'unixepoch')) as month,
            ZASSET.ZDIRECTORY,
            ZASSET.ZUNIFORMTYPEIDENTIFIER,
            ZADDITIONALASSETATTRIBUTES.ZORIGINALFILENAME,
            datetime(ZASSET.ZDATECREATED + 978307200, 'unixepoch') as created_date
        FROM ZASSET
        LEFT JOIN ZADDITIONALASSETATTRIBUTES ON ZASSET.Z_PK = ZADDITIONALASSETATTRIBUTES.ZASSET
        WHERE ZASSET.ZTRASHEDSTATE = 0
        AND ZASSET.ZFILENAME IS NOT NULL
        ORDER BY ZASSET.ZDATECREATED DESC
        """
        return cursor.execute(query).fetchall()
    except sqlite3.Error as e:
        raise Exception(f"Failed to query database: {str(e)}")

def find_photo_file(photo_info):
    """写真・動画ファイルを探す（ファイルタイプに基づいて最適なファイルを選択）"""
    zuuid, filename, _, _, directory, uniform_type, original_filename, created_date = photo_info
    print(f"\nFile details:")
    print(f"UUID: {zuuid}")
    print(f"Filename: {filename}")
    print(f"Original Filename: {original_filename}")
    print(f"Created Date: {created_date}")
    print(f"Uniform Type Identifier: {uniform_type}")

    first_letter = zuuid[0]
    found_files = []

    # 検索パターン（優先順位順）
    search_paths = [
        # オリジナルファイル優先
        os.path.join(PHOTOS_LIBRARY, 'originals', first_letter),
        os.path.join(PHOTOS_LIBRARY, 'originals', directory if directory else first_letter),
        os.path.join(PHOTOS_LIBRARY, 'originals'),
        # バックアップファイル
        os.path.join(PHOTOS_LIBRARY, 'resources/media/master'),
        os.path.join(PHOTOS_LIBRARY, 'resources/media/video'),
        # その他のパス
        os.path.join(PHOTOS_LIBRARY, 'resources/derivatives', first_letter),
        os.path.join(PHOTOS_LIBRARY, 'resources/derivatives/masters', first_letter),
        PHOTOS_LIBRARY
    ]

    for base_path in search_paths:
        if not os.path.exists(base_path):
            continue

        if os.path.isdir(base_path):
            for root, _, files in os.walk(base_path):
                for file in files:
                    if zuuid in file:
                        full_path = os.path.join(root, file)
                        found_files.append(full_path)
        else:
            found_files.append(base_path)

    if not found_files:
        print("File not found in any location")
        return None

    def prioritize_file(file_path):
        ext = os.path.splitext(file_path)[1].lower()
        video_extensions = ['.mp4', '.mov', '.avi', '.mkv', '.m4v']
        size_mb = os.path.getsize(file_path) / (1024 * 1024)  # サイズをMBで取得

        # ファイルサイズが小さすぎる場合（1MB未満）は低い優先度に
        if size_mb < 1:
            return 0

        # originals ディレクトリにあるファイルを最優先
        if '/originals/' in file_path:
            if ext in video_extensions:
                return 10  # 動画ファイルを最優先
            return 9

        # media/master または media/video ディレクトリのファイル
        if '/resources/media/' in file_path:
            if size_mb > 10:  # 10MB以上のファイルを優先
                return 8
            return 7

        # その他のファイル
        if ext == '.heic':
            return 6
        elif ext == '.png':
            return 5
        elif ext in ['.jpg', '.jpeg', '.raw', '.cr2', '.arw']:
            return 4
        elif ext in video_extensions and size_mb > 10:
            return 3

        return 1  # その他の小さいファイル

    prioritized_files = sorted(found_files, key=prioritize_file, reverse=True)
    selected_file = prioritized_files[0]
    print(f"Found file at: {selected_file}")
    return selected_file

def upload_to_nas(conn, photo_path, remote_path):
    """NASにアップロード"""
    try:
        if not os.path.exists(photo_path):
            print(f"File not found: {photo_path}")
            return False

        # リモートパスの準備
        remote_dir = os.path.dirname(remote_path)
        filename = os.path.basename(remote_path)

        # リモートディレクトリが存在することを確認（必要に応じて作成）
        dir_parts = remote_dir.split('/')
        current_dir = ''
        for part in dir_parts:
            if part:
                current_dir = f"{current_dir}/{part}" if current_dir else part
                try:
                    conn.createDirectory(NAS_SHARE, current_dir)
                except:
                    pass  # ディレクトリが既に存在する場合は無視

        # ファイルアップロード
        with open(photo_path, 'rb') as file:
            conn.storeFile(NAS_SHARE, remote_path, file)

        return True
    except Exception as e:
        print(f"Error uploading {photo_path}: {str(e)}")
        return False

def main():
    try:
        # ホスト名の解決を試みる
        import socket
        try:
            # まずIPアドレスが直接設定されているか確認
            target_host = NAS_IP if NAS_IP else NAS_HOST
            # IPアドレスの解決を試みる
            ip_address = socket.gethostbyname(target_host)
            print(f"Resolved IP address: {ip_address}")
        except socket.gaierror as e:
            print(f"Failed to resolve hostname: {e}")
            print("Please check your NAS hostname or set the IP address directly in NAS_IP")
            return

        # SMB接続の確立
        smb_conn = SMBConnection(
            NAS_USERNAME,
            NAS_PASSWORD,
            'local-machine',  # ローカルのマシン名
            NAS_HOST,         # NASのホスト名
            use_ntlm_v2=True,
            is_direct_tcp=True
        )

        print(f"Attempting to connect to {ip_address}...")
        print(f"Using credentials - Username: {NAS_USERNAME}, Share: {NAS_SHARE}")
        try:
            connected = smb_conn.connect(ip_address, 445)  # SMBのデフォルトポート
            if not connected:
                raise Exception("Connection returned False")
        except Exception as e:
            raise Exception(f"Failed to connect to NAS: {str(e)}")

        # 接続テスト：共有の一覧を取得
        try:
            shares = smb_conn.listShares()
            print("Available shares:")
            for share in shares:
                print(f"- {share.name}")
        except Exception as e:
            raise Exception(f"Connected but failed to list shares: {str(e)}")

        print("Connected to NAS successfully")

        # 写真ライブラリに接続
        print("Connecting to Photos library...")
        conn = connect_to_photos_library()
        print("Successfully connected to Photos library")

        # 写真情報を取得
        print("\nFetching photos information...")
        photos = get_photos_info(conn)
        print(f"Found {len(photos)} photos")

        # すべての写真を処理
        print("\nProcessing all photos...")
        success_count = 0
        total_count = len(photos)

        for i, photo_info in enumerate(photos, 1):
            filename = photo_info[1]
            year = photo_info[2]
            month = photo_info[3]
            original_filename = photo_info[6]

            print(f"\nProcessing {i}/{total_count}: {filename}")
            print(f"Original filename: {original_filename}")

            # 写真ファイルを探す
            photo_path = find_photo_file(photo_info)

            if not photo_path:
                print(f"Could not find file: {filename}")
                continue

            # NASでのパス
            upload_filename = original_filename if original_filename else filename
            remote_path = f"{NAS_PATH}/{year}/{month}/{upload_filename}"

            # アップロード実行
            if upload_to_nas(smb_conn, photo_path, remote_path):
                print(f"Uploaded: {upload_filename} to {remote_path}")
                success_count += 1

            # 進捗状況の表示
            if i % 10 == 0:
                print(f"\nProgress: {i}/{total_count} files processed ({success_count} successful)")

        print(f"\nProcessing complete. Successfully processed {success_count} out of {total_count} files.")

        # 接続のクローズ
        smb_conn.close()
        conn.close()

    except Exception as e:
        print(f"Error occurred: {str(e)}")
        raise

if __name__ == "__main__":
    main()
